package redis

import (
	"context"
	"fmt"
	"strings"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const insertHashScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then
  return 0
end
redis.call('HSET', KEYS[1], unpack(ARGV))
return 1
`

// Insert creates Redis hashes atomically and rejects existing keys.
func (c *Connection) Insert(
	ctx context.Context,
	request *providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "insert request is required")
	}
	if len(request.GetReturningFields()) > 0 {
		return nil, status.Error(codes.InvalidArgument, "Redis does not support generated or returning values")
	}
	resolved, err := c.resolveTable(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}
	if !resolved.table.Updatable {
		return nil, status.Errorf(codes.FailedPrecondition, "entity %q is read-only", resolved.table.Name)
	}
	rows := request.GetRows()
	if rows == nil || len(rows.GetFields()) == 0 || len(rows.GetTuples()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "insert fields and tuples are required")
	}
	columns, keyIndex, err := mutationColumns(resolved.table, rows.GetFields())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "resolve Redis insert fields: %v", err)
	}
	if keyIndex < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "insert requires key column %q", resolved.table.Key.Name)
	}
	if len(columns) == 1 {
		return nil, status.Error(codes.InvalidArgument, "Redis hash insert requires at least one non-key field")
	}
	if err := validateRequiredInsertColumns(resolved.table, columns); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "resolve Redis insert fields: %v", err)
	}

	var affected uint64
	for rowIndex, tuple := range rows.GetTuples() {
		if tuple == nil || len(tuple.GetValues()) != len(columns) {
			return nil, status.Errorf(codes.InvalidArgument, "insert tuple %d has %d values, want %d", rowIndex, len(tuple.GetValues()), len(columns))
		}
		keyPart, null, err := encodeRedisValue(tuple.GetValues()[keyIndex], resolved.table.Key.Type)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "insert tuple %d key: %v", rowIndex, err)
		}
		if null {
			return nil, status.Errorf(codes.InvalidArgument, "insert tuple %d key cannot be null", rowIndex)
		}
		arguments := make([]any, 0, (len(columns)-1)*2)
		for index, column := range columns {
			if index == keyIndex {
				continue
			}
			raw, isNull, err := encodeRedisValue(tuple.GetValues()[index], column.Type)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "insert tuple %d column %q: %v", rowIndex, column.Name, err)
			}
			if isNull {
				if !column.Nullable {
					return nil, status.Errorf(codes.InvalidArgument, "insert tuple %d column %q cannot be null", rowIndex, column.Name)
				}
				continue
			}
			arguments = append(arguments, column.Name, raw)
		}
		if len(arguments) == 0 {
			return nil, status.Errorf(codes.InvalidArgument, "insert tuple %d has no non-null hash fields", rowIndex)
		}
		created, err := resolved.client.EvalInt(ctx, insertHashScript, []string{resolved.table.KeyPrefix + keyPart}, arguments...)
		if err != nil {
			return nil, operationError("insert Redis hash", err)
		}
		if created == 0 {
			return nil, status.Errorf(codes.AlreadyExists, "Redis entity key %q already exists", keyPart)
		}
		affected++
	}
	return &providerv1.InsertResponse{AffectedRows: &affected}, nil
}

// Update applies literal assignments to selected Redis hashes.
func (c *Connection) Update(
	ctx context.Context,
	request *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	if request == nil || request.GetFilter() == nil {
		return nil, status.Error(codes.InvalidArgument, "Redis update requires a filter")
	}
	resolved, err := c.resolveTable(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}
	if !resolved.table.Updatable {
		return nil, status.Errorf(codes.FailedPrecondition, "entity %q is read-only", resolved.table.Name)
	}
	sets, deletes, err := updateAssignments(resolved.table, request.GetAssignments())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "resolve Redis assignments: %v", err)
	}
	keys, err := resolveKeys(ctx, resolved, request.GetFilter())
	if err != nil {
		return nil, err
	}
	var affected uint64
	for _, key := range keys {
		row, exists, err := readHashRow(ctx, resolved, key)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		matches, err := evaluateFilter(row, request.GetFilter())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "evaluate Redis update filter: %v", err)
		}
		if !matches {
			continue
		}
		if len(sets) > 0 {
			if err := resolved.client.HSet(ctx, key, sets); err != nil {
				return nil, operationError("update Redis hash", err)
			}
		}
		if len(deletes) > 0 {
			if _, err := resolved.client.HDel(ctx, key, deletes...); err != nil {
				return nil, operationError("delete Redis hash fields", err)
			}
		}
		affected++
	}
	return &providerv1.UpdateResponse{AffectedRows: &affected}, nil
}

// Delete removes selected Redis hashes.
func (c *Connection) Delete(
	ctx context.Context,
	request *providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	if request == nil || request.GetFilter() == nil {
		return nil, status.Error(codes.InvalidArgument, "Redis delete requires a filter")
	}
	resolved, err := c.resolveTable(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}
	if !resolved.table.Updatable {
		return nil, status.Errorf(codes.FailedPrecondition, "entity %q is read-only", resolved.table.Name)
	}
	keys, err := resolveKeys(ctx, resolved, request.GetFilter())
	if err != nil {
		return nil, err
	}
	selected := make([]string, 0, len(keys))
	for _, key := range keys {
		row, exists, err := readHashRow(ctx, resolved, key)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		matches, err := evaluateFilter(row, request.GetFilter())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "evaluate Redis delete filter: %v", err)
		}
		if matches {
			selected = append(selected, key)
		}
	}
	var affected uint64
	if len(selected) > 0 {
		deleted, err := resolved.client.Del(ctx, selected...)
		if err != nil {
			return nil, operationError("delete Redis hashes", err)
		}
		affected = uint64(deleted)
	}
	return &providerv1.DeleteResponse{AffectedRows: &affected}, nil
}

func mutationColumns(table *TableConfig, fields []*providerv1.Field) ([]ColumnConfig, int, error) {
	columns := make([]ColumnConfig, 0, len(fields))
	keyIndex := -1
	seen := make(map[string]struct{}, len(fields))
	for index, field := range fields {
		if field == nil || strings.TrimSpace(field.GetName()) == "" {
			return nil, -1, fmt.Errorf("field name is required")
		}
		column := table.columns[strings.ToUpper(field.GetName())]
		if column == nil {
			return nil, -1, fmt.Errorf("column %q was not found", field.GetName())
		}
		key := strings.ToUpper(column.Name)
		if _, exists := seen[key]; exists {
			return nil, -1, fmt.Errorf("column %q is repeated", column.Name)
		}
		seen[key] = struct{}{}
		columns = append(columns, *column)
		if strings.EqualFold(column.Name, table.Key.Name) {
			keyIndex = index
		}
	}
	return columns, keyIndex, nil
}

func validateRequiredInsertColumns(table *TableConfig, columns []ColumnConfig) error {
	present := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		present[strings.ToUpper(column.Name)] = struct{}{}
	}
	for _, field := range table.Fields {
		if field.Nullable {
			continue
		}
		if _, exists := present[strings.ToUpper(field.Name)]; !exists {
			return fmt.Errorf("insert requires non-null column %q", field.Name)
		}
	}
	return nil
}

func updateAssignments(table *TableConfig, assignments []*providerv1.Assignment) (map[string]string, []string, error) {
	if len(assignments) == 0 {
		return nil, nil, fmt.Errorf("at least one assignment is required")
	}
	sets := make(map[string]string)
	var deletes []string
	seen := make(map[string]struct{}, len(assignments))
	for _, assignment := range assignments {
		if assignment == nil || strings.TrimSpace(assignment.GetField()) == "" {
			return nil, nil, fmt.Errorf("assignment field is required")
		}
		column := table.columns[strings.ToUpper(assignment.GetField())]
		if column == nil {
			return nil, nil, fmt.Errorf("column %q was not found", assignment.GetField())
		}
		if strings.EqualFold(column.Name, table.Key.Name) || !column.Updatable {
			return nil, nil, fmt.Errorf("column %q is not updatable", column.Name)
		}
		key := strings.ToUpper(column.Name)
		if _, exists := seen[key]; exists {
			return nil, nil, fmt.Errorf("column %q is assigned more than once", column.Name)
		}
		seen[key] = struct{}{}
		literal := assignment.GetValue().GetLiteral()
		if literal == nil {
			return nil, nil, fmt.Errorf("assignment %q requires a literal", column.Name)
		}
		raw, isNull, err := encodeRedisValue(literal.GetValue(), column.Type)
		if err != nil {
			return nil, nil, fmt.Errorf("assignment %q: %w", column.Name, err)
		}
		if isNull {
			if !column.Nullable {
				return nil, nil, fmt.Errorf("column %q cannot be null", column.Name)
			}
			deletes = append(deletes, column.Name)
			continue
		}
		sets[column.Name] = raw
	}
	return sets, deletes, nil
}
