package cassandra

import (
	"context"
	"fmt"
	"strings"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Query translates a supported logical query to parameterized CQL.
func (c *Connection) Query(
	ctx context.Context,
	request *providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "query request is required")
	}
	if request.GetOffset() > 0 {
		return nil, status.Error(codes.InvalidArgument, "Cassandra does not support offset")
	}

	entity, err := c.resolveEntity(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}

	projections, err := planQueryProjections(entity.table, request.GetProjections())
	if err != nil {
		entity.Close()
		return nil, status.Errorf(codes.InvalidArgument, "plan Cassandra projections: %v", err)
	}
	if len(projections) == 0 {
		entity.Close()
		return nil, status.Error(codes.InvalidArgument, "query requires at least one projected column")
	}

	filter, values, err := buildFilter(entity.table, request.GetFilter())
	if err != nil {
		entity.Close()
		return nil, status.Errorf(codes.InvalidArgument, "plan Cassandra filter: %v", err)
	}
	ordering, err := buildOrdering(entity.table, request.GetOrderBy())
	if err != nil {
		entity.Close()
		return nil, status.Errorf(codes.InvalidArgument, "plan Cassandra ordering: %v", err)
	}
	limit, err := queryLimit(request.Limit)
	if err != nil {
		entity.Close()
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	var statement strings.Builder
	fmt.Fprintf(
		&statement,
		"SELECT %s FROM %s",
		selectColumns(projections),
		entity.qualifiedTable(),
	)
	if filter != "" {
		statement.WriteString(" WHERE ")
		statement.WriteString(filter)
	}
	if ordering != "" {
		statement.WriteString(" ORDER BY ")
		statement.WriteString(ordering)
	}
	statement.WriteString(limit)

	iterator := entity.session.Query(
		ctx,
		statement.String(),
		values,
		queryPageSize(request.BatchSize),
	)

	return newCassandraResultStream(
		entity,
		iterator,
		projections,
		queryPageSize(request.BatchSize),
	), nil
}
