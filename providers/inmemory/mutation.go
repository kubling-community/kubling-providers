package inmemory

import (
	"context"
	"fmt"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Insert adds tasks to the selected in-memory store.
func (c *Connection) Insert(
	ctx context.Context,
	request *providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	if err := c.lockOpen(); err != nil {
		return nil, err
	}
	defer c.unlockOpen()

	if err := validateMutableTaskEntity(request.GetEntity()); err != nil {
		return nil, err
	}

	drafts, err := parseInsertRows(request.GetRows())
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"parse inserted rows: %v",
			err,
		)
	}

	returningFields, err :=
		resolveTaskFields(request.GetReturningFields())
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"resolve returning fields: %v",
			err,
		)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.store.mu.Lock()
	inserted, nextID, err := prepareInsertedTasks(
		c.store.tasks,
		c.store.nextID,
		drafts,
	)
	if err == nil {
		for _, row := range inserted {
			c.store.tasks[row.ID] = row
		}
		c.store.nextID = nextID
	}
	c.store.mu.Unlock()

	if err != nil {
		return nil, err
	}

	response := &providerv1.InsertResponse{
		AffectedRows: affectedRows(len(inserted)),
	}

	if len(returningFields) > 0 {
		generatedTuples := make(
			[]*providerv1.Tuple,
			0,
			len(inserted),
		)
		for _, row := range inserted {
			tuple, err := taskTuple(row, returningFields)
			if err != nil {
				return nil, status.Errorf(
					codes.Internal,
					"build generated values: %v",
					err,
				)
			}

			generatedTuples = append(generatedTuples, tuple)
		}

		response.GeneratedValues = &providerv1.TupleBatch{
			Fields: providerFields(returningFields),
			Tuples: generatedTuples,
		}
	}

	return response, nil
}

// Update modifies tasks selected by the request filter.
func (c *Connection) Update(
	ctx context.Context,
	request *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	if err := c.lockOpen(); err != nil {
		return nil, err
	}
	defer c.unlockOpen()

	if err := validateMutableTaskEntity(request.GetEntity()); err != nil {
		return nil, err
	}

	assignments, err := validateAssignments(request.GetAssignments())
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"validate assignments: %v",
			err,
		)
	}

	c.store.mu.Lock()
	updated := make(map[string]task)

	for id, row := range c.store.tasks {
		if err := ctx.Err(); err != nil {
			c.store.mu.Unlock()
			return nil, err
		}

		matches, err := evaluateFilter(taskRow(row), request.GetFilter())
		if err != nil {
			c.store.mu.Unlock()
			return nil, status.Errorf(
				codes.InvalidArgument,
				"evaluate update filter: %v",
				err,
			)
		}
		if !matches {
			continue
		}

		next := row
		for _, assignment := range assignments {
			value, err := evaluateExpression(
				taskRow(row),
				assignment.GetValue(),
			)
			if err != nil {
				c.store.mu.Unlock()
				return nil, status.Errorf(
					codes.InvalidArgument,
					"evaluate assignment %q: %v",
					assignment.GetField(),
					err,
				)
			}

			if err := assignTaskValue(
				&next,
				assignment.GetField(),
				value,
			); err != nil {
				c.store.mu.Unlock()
				return nil, status.Errorf(
					codes.InvalidArgument,
					"apply assignment %q: %v",
					assignment.GetField(),
					err,
				)
			}
		}

		updated[id] = next
	}

	for id, row := range updated {
		c.store.tasks[id] = row
	}
	c.store.mu.Unlock()

	return &providerv1.UpdateResponse{
		AffectedRows: affectedRows(len(updated)),
	}, nil
}

// Delete removes tasks selected by the request filter.
func (c *Connection) Delete(
	ctx context.Context,
	request *providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	if err := c.lockOpen(); err != nil {
		return nil, err
	}
	defer c.unlockOpen()

	if err := validateMutableTaskEntity(request.GetEntity()); err != nil {
		return nil, err
	}

	c.store.mu.Lock()
	deletedIDs := make([]string, 0)

	for id, row := range c.store.tasks {
		if err := ctx.Err(); err != nil {
			c.store.mu.Unlock()
			return nil, err
		}

		matches, err := evaluateFilter(taskRow(row), request.GetFilter())
		if err != nil {
			c.store.mu.Unlock()
			return nil, status.Errorf(
				codes.InvalidArgument,
				"evaluate delete filter: %v",
				err,
			)
		}
		if matches {
			deletedIDs = append(deletedIDs, id)
		}
	}

	for _, id := range deletedIDs {
		delete(c.store.tasks, id)
	}
	c.store.mu.Unlock()

	return &providerv1.DeleteResponse{
		AffectedRows: affectedRows(len(deletedIDs)),
	}, nil
}

func parseInsertRows(
	rows *providerv1.TupleBatch,
) ([]task, error) {
	if rows == nil {
		return nil, fmt.Errorf("rows are required")
	}

	fieldNames := make([]string, 0, len(rows.GetFields()))
	seenFields := make(map[string]struct{}, len(rows.GetFields()))

	for _, field := range rows.GetFields() {
		if field == nil || field.GetName() == "" {
			return nil, fmt.Errorf("row field name is required")
		}

		taskField, found := taskFieldByName(field.GetName())
		if !found {
			return nil, fmt.Errorf("unknown TASK field %q", field.GetName())
		}
		if _, duplicate := seenFields[taskField.name]; duplicate {
			return nil, fmt.Errorf(
				"duplicate TASK field %q",
				field.GetName(),
			)
		}

		seenFields[taskField.name] = struct{}{}
		fieldNames = append(fieldNames, taskField.name)
	}

	drafts := make([]task, 0, len(rows.GetTuples()))
	for tupleIndex, tuple := range rows.GetTuples() {
		if tuple == nil {
			return nil, fmt.Errorf("tuple %d is required", tupleIndex)
		}
		if len(tuple.GetValues()) != len(fieldNames) {
			return nil, fmt.Errorf(
				"tuple %d has %d values for %d fields",
				tupleIndex,
				len(tuple.GetValues()),
				len(fieldNames),
			)
		}

		row := task{ProjectID: "project-1"}
		titleSet := false

		for fieldIndex, fieldName := range fieldNames {
			value := tuple.GetValues()[fieldIndex]

			switch fieldName {
			case "id":
				if isNullValue(value) {
					continue
				}

				id, err := stringFromValue(value)
				if err != nil {
					return nil, fmt.Errorf(
						"tuple %d field id: %w",
						tupleIndex,
						err,
					)
				}
				row.ID = id
			case "project_id":
				projectID, err := stringFromValue(value)
				if err != nil {
					return nil, fmt.Errorf(
						"tuple %d field project_id: %w",
						tupleIndex,
						err,
					)
				}
				row.ProjectID = projectID
			case "title":
				title, err := stringFromValue(value)
				if err != nil {
					return nil, fmt.Errorf(
						"tuple %d field title: %w",
						tupleIndex,
						err,
					)
				}
				row.Title = title
				titleSet = true
			case "description":
				if isNullValue(value) {
					row.Description = nil
					continue
				}
				description, err := clobFromValue(value)
				if err != nil {
					return nil, fmt.Errorf(
						"tuple %d field description: %w",
						tupleIndex,
						err,
					)
				}
				row.Description = &description
			case "completed":
				completed, err := booleanFromValue(value)
				if err != nil {
					return nil, fmt.Errorf(
						"tuple %d field completed: %w",
						tupleIndex,
						err,
					)
				}
				row.Completed = completed
			case "priority":
				priority, err := integerFromValue(value)
				if err != nil {
					return nil, fmt.Errorf(
						"tuple %d field priority: %w",
						tupleIndex,
						err,
					)
				}
				row.Priority = priority
			case "estimate_hours":
				if isNullValue(value) {
					row.EstimateHours = nil
					continue
				}
				estimateHours, err := floatFromValue(value)
				if err != nil {
					return nil, fmt.Errorf(
						"tuple %d field estimate_hours: %w",
						tupleIndex,
						err,
					)
				}
				row.EstimateHours = &estimateHours
			case "due_at":
				if isNullValue(value) {
					row.DueAt = nil
					continue
				}
				dueAt, err := timestampFromValue(value)
				if err != nil {
					return nil, fmt.Errorf(
						"tuple %d field due_at: %w",
						tupleIndex,
						err,
					)
				}
				row.DueAt = &dueAt
			}
		}

		if !titleSet {
			return nil, fmt.Errorf(
				"tuple %d does not provide required field title",
				tupleIndex,
			)
		}

		drafts = append(drafts, row)
	}

	return drafts, nil
}

func prepareInsertedTasks(
	existing map[string]task,
	nextID uint64,
	drafts []task,
) ([]task, uint64, error) {
	inserted := make([]task, 0, len(drafts))
	pendingIDs := make(map[string]struct{}, len(drafts))

	for _, draft := range drafts {
		if draft.ID == "" {
			for {
				draft.ID = fmt.Sprintf("task-%d", nextID)
				nextID++

				_, exists := existing[draft.ID]
				_, pending := pendingIDs[draft.ID]
				if !exists && !pending {
					break
				}
			}
		}

		if _, exists := existing[draft.ID]; exists {
			return nil, nextID, status.Errorf(
				codes.AlreadyExists,
				"TASK %q already exists",
				draft.ID,
			)
		}
		if _, duplicate := pendingIDs[draft.ID]; duplicate {
			return nil, nextID, status.Errorf(
				codes.AlreadyExists,
				"TASK %q appears more than once",
				draft.ID,
			)
		}

		pendingIDs[draft.ID] = struct{}{}
		inserted = append(inserted, draft)
	}

	return inserted, nextID, nil
}

func resolveTaskFields(names []string) ([]entityField, error) {
	fields := make([]entityField, 0, len(names))
	for _, name := range names {
		field, found := taskFieldByName(name)
		if !found {
			return nil, fmt.Errorf("unknown TASK field %q", name)
		}

		fields = append(fields, field)
	}

	return fields, nil
}

func validateAssignments(
	assignments []*providerv1.Assignment,
) ([]*providerv1.Assignment, error) {
	if len(assignments) == 0 {
		return nil, fmt.Errorf("at least one assignment is required")
	}

	seen := make(map[string]struct{}, len(assignments))
	for _, assignment := range assignments {
		if assignment == nil || assignment.GetField() == "" {
			return nil, fmt.Errorf("assignment field is required")
		}

		field, found := taskFieldByName(assignment.GetField())
		if !found {
			return nil, fmt.Errorf(
				"unknown TASK field %q",
				assignment.GetField(),
			)
		}
		if field.name == "id" {
			return nil, fmt.Errorf("field id cannot be updated")
		}
		if assignment.GetValue() == nil {
			return nil, fmt.Errorf(
				"assignment %q value is required",
				assignment.GetField(),
			)
		}
		if _, duplicate := seen[field.name]; duplicate {
			return nil, fmt.Errorf(
				"field %q is assigned more than once",
				assignment.GetField(),
			)
		}

		seen[field.name] = struct{}{}
	}

	return assignments, nil
}

func assignTaskValue(
	row *task,
	fieldName string,
	value *kublingv1.Value,
) error {
	if isNullValue(value) {
		switch {
		case equalName(fieldName, "description"):
			row.Description = nil
		case equalName(fieldName, "estimate_hours"):
			row.EstimateHours = nil
		case equalName(fieldName, "due_at"):
			row.DueAt = nil
		default:
			return fmt.Errorf("TASK field %q cannot be null", fieldName)
		}

		return nil
	}

	switch {
	case equalName(fieldName, "project_id"):
		projectID, err := stringFromValue(value)
		if err != nil {
			return err
		}
		row.ProjectID = projectID
	case equalName(fieldName, "title"):
		title, err := stringFromValue(value)
		if err != nil {
			return err
		}
		row.Title = title
	case equalName(fieldName, "description"):
		description, err := clobFromValue(value)
		if err != nil {
			return err
		}
		row.Description = &description
	case equalName(fieldName, "completed"):
		completed, err := booleanFromValue(value)
		if err != nil {
			return err
		}
		row.Completed = completed
	case equalName(fieldName, "priority"):
		priority, err := integerFromValue(value)
		if err != nil {
			return err
		}
		row.Priority = priority
	case equalName(fieldName, "estimate_hours"):
		estimateHours, err := floatFromValue(value)
		if err != nil {
			return err
		}
		row.EstimateHours = &estimateHours
	case equalName(fieldName, "due_at"):
		dueAt, err := timestampFromValue(value)
		if err != nil {
			return err
		}
		row.DueAt = &dueAt
	default:
		return fmt.Errorf("field %q cannot be updated", fieldName)
	}

	return nil
}

func stringFromValue(value *kublingv1.Value) (string, error) {
	kind, ok := value.GetKind().(*kublingv1.Value_StringValue)
	if !ok {
		return "", fmt.Errorf("expected string value")
	}

	return kind.StringValue, nil
}

func booleanFromValue(value *kublingv1.Value) (bool, error) {
	kind, ok := value.GetKind().(*kublingv1.Value_BooleanValue)
	if !ok {
		return false, fmt.Errorf("expected boolean value")
	}

	return kind.BooleanValue, nil
}

func integerFromValue(value *kublingv1.Value) (int32, error) {
	kind, ok := value.GetKind().(*kublingv1.Value_IntegerValue)
	if !ok {
		return 0, fmt.Errorf("expected integer value")
	}

	return kind.IntegerValue, nil
}

func clobFromValue(value *kublingv1.Value) (string, error) {
	kind, ok := value.GetKind().(*kublingv1.Value_ClobValue)
	if !ok || kind.ClobValue == nil {
		return "", fmt.Errorf("expected clob value")
	}

	return kind.ClobValue.GetData(), nil
}

func floatFromValue(value *kublingv1.Value) (float32, error) {
	kind, ok := value.GetKind().(*kublingv1.Value_FloatValue)
	if !ok {
		return 0, fmt.Errorf("expected float value")
	}

	return kind.FloatValue, nil
}

func timestampFromValue(value *kublingv1.Value) (string, error) {
	kind, ok := value.GetKind().(*kublingv1.Value_TimestampValue)
	if !ok {
		return "", fmt.Errorf("expected timestamp value")
	}

	return kind.TimestampValue, nil
}

func affectedRows(count int) *uint64 {
	value := uint64(count)

	return &value
}
