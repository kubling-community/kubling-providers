package cassandra

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/cassandra-gocql-driver/v2"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Insert translates tuples to individual parameterized Cassandra inserts.
func (c *Connection) Insert(
	ctx context.Context,
	request *providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "insert request is required")
	}
	if len(request.GetReturningFields()) > 0 {
		return nil, status.Error(
			codes.InvalidArgument,
			"Cassandra does not support generated or returning values",
		)
	}

	entity, err := c.resolveEntity(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}
	defer entity.Close()

	rows := request.GetRows()
	if rows == nil || len(rows.GetFields()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "insert fields are required")
	}
	if len(rows.GetTuples()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "insert tuples are required")
	}

	columns, err := resolveMutationFields(entity.table, rows.GetFields())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "resolve insert fields: %v", err)
	}
	quotedColumns := make([]string, 0, len(columns))
	markers := make([]string, 0, len(columns))
	for _, column := range columns {
		quotedColumns = append(quotedColumns, quoteIdentifier(column.Name))
		markers = append(markers, "?")
	}
	statement := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		entity.qualifiedTable(),
		strings.Join(quotedColumns, ", "),
		strings.Join(markers, ", "),
	)

	for rowIndex, tuple := range rows.GetTuples() {
		if tuple == nil || len(tuple.GetValues()) != len(columns) {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"insert tuple %d has %d values, want %d",
				rowIndex,
				len(tuple.GetValues()),
				len(columns),
			)
		}
		values := make([]any, 0, len(columns))
		for columnIndex, column := range columns {
			value, err := providerToNative(tuple.GetValues()[columnIndex], column.Type)
			if err != nil {
				return nil, status.Errorf(
					codes.InvalidArgument,
					"insert tuple %d column %q: %v",
					rowIndex,
					column.Name,
					err,
				)
			}
			values = append(values, value)
		}
		if err := entity.session.Exec(ctx, statement, values); err != nil {
			return nil, operationError("execute Cassandra insert", err)
		}
	}

	affectedRows := uint64(len(rows.GetTuples()))
	return &providerv1.InsertResponse{AffectedRows: &affectedRows}, nil
}

// Update translates literal assignments and a required filter to CQL.
func (c *Connection) Update(
	ctx context.Context,
	request *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "update request is required")
	}
	if request.GetFilter() == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"Cassandra update requires a filter",
		)
	}

	entity, err := c.resolveEntity(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}
	defer entity.Close()

	assignments, values, err := buildAssignments(entity.table, request.GetAssignments())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "resolve update assignments: %v", err)
	}
	filter, filterValues, err := buildFilter(entity.table, request.GetFilter())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan Cassandra update filter: %v", err)
	}
	if filter == "" {
		return nil, status.Error(codes.InvalidArgument, "Cassandra update requires a filter")
	}
	values = append(values, filterValues...)
	statement := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		entity.qualifiedTable(),
		assignments,
		filter,
	)
	if err := entity.session.Exec(ctx, statement, values); err != nil {
		return nil, operationError("execute Cassandra update", err)
	}

	return &providerv1.UpdateResponse{}, nil
}

// Delete translates a required filter to parameterized CQL.
func (c *Connection) Delete(
	ctx context.Context,
	request *providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "delete request is required")
	}
	if request.GetFilter() == nil {
		return nil, status.Error(
			codes.InvalidArgument,
			"Cassandra delete requires a filter",
		)
	}

	entity, err := c.resolveEntity(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}
	defer entity.Close()

	filter, values, err := buildFilter(entity.table, request.GetFilter())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan Cassandra delete filter: %v", err)
	}
	if filter == "" {
		return nil, status.Error(codes.InvalidArgument, "Cassandra delete requires a filter")
	}
	statement := fmt.Sprintf(
		"DELETE FROM %s WHERE %s",
		entity.qualifiedTable(),
		filter,
	)
	if err := entity.session.Exec(ctx, statement, values); err != nil {
		return nil, operationError("execute Cassandra delete", err)
	}

	return &providerv1.DeleteResponse{}, nil
}

func resolveMutationFields(
	table *gocql.TableMetadata,
	fields []*providerv1.Field,
) ([]*gocql.ColumnMetadata, error) {
	columns := make([]*gocql.ColumnMetadata, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field == nil || strings.TrimSpace(field.GetName()) == "" {
			return nil, fmt.Errorf("insert field name is required")
		}
		column := findColumn(table, field.GetName())
		if column == nil {
			return nil, fmt.Errorf("column %q was not found", field.GetName())
		}
		key := strings.ToUpper(column.Name)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("column %q is repeated", column.Name)
		}
		seen[key] = struct{}{}
		columns = append(columns, column)
	}

	return columns, nil
}

func buildAssignments(
	table *gocql.TableMetadata,
	assignments []*providerv1.Assignment,
) (string, []any, error) {
	if len(assignments) == 0 {
		return "", nil, fmt.Errorf("at least one assignment is required")
	}

	parts := make([]string, 0, len(assignments))
	values := make([]any, 0, len(assignments))
	seen := make(map[string]struct{}, len(assignments))
	for _, assignment := range assignments {
		if assignment == nil || strings.TrimSpace(assignment.GetField()) == "" {
			return "", nil, fmt.Errorf("assignment field is required")
		}
		column := findColumn(table, assignment.GetField())
		if column == nil {
			return "", nil, fmt.Errorf("column %q was not found", assignment.GetField())
		}
		if column.Kind == gocql.ColumnPartitionKey || column.Kind == gocql.ColumnClusteringKey {
			return "", nil, fmt.Errorf("primary key column %q cannot be updated", column.Name)
		}
		key := strings.ToUpper(column.Name)
		if _, exists := seen[key]; exists {
			return "", nil, fmt.Errorf("column %q is assigned more than once", column.Name)
		}
		seen[key] = struct{}{}

		literal := assignment.GetValue().GetLiteral()
		if literal == nil {
			return "", nil, fmt.Errorf("assignment %q requires a literal value", column.Name)
		}
		value, err := providerToNative(literal.GetValue(), column.Type)
		if err != nil {
			return "", nil, fmt.Errorf("assignment %q: %w", column.Name, err)
		}
		parts = append(parts, quoteIdentifier(column.Name)+" = ?")
		values = append(values, value)
	}

	return strings.Join(parts, ", "), values, nil
}
