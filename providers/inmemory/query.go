package inmemory

import (
	"context"
	"fmt"
	"slices"
	"sort"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/inmemory/internal/querylog"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const defaultBatchSize = 100

type plannedProjection struct {
	expression *providerv1.Expression
	field      *providerv1.Field
}

type plannedRow struct {
	row         entityRow
	orderValues []*kublingv1.Value
}

// Query evaluates a logical query against a snapshot of the in-memory store.
func (c *Connection) Query(
	ctx context.Context,
	request *providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	if err := c.lockOpen(); err != nil {
		return nil, err
	}
	defer c.unlockOpen()

	entity, err := resolveEntity(request.GetEntity())
	if err != nil {
		return nil, err
	}

	projections, err := planProjections(entity, request.GetProjections())
	if err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"plan projections: %v",
			err,
		)
	}
	if !slices.Contains(
		request.GetAcceptedFeatures(),
		grpcfeatures.ArrayValuesV1,
	) {
		for _, projection := range projections {
			if projection.field.GetType() == kublingv1.ValueType_VALUE_TYPE_ARRAY {
				return nil, status.Errorf(
					codes.FailedPrecondition,
					"projection %q requires feature %q",
					projection.field.GetName(),
					grpcfeatures.ArrayValuesV1,
				)
			}
		}
	}

	rows := c.store.snapshot(entity)
	if aggregateQuery(request) {
		return c.queryAggregates(ctx, request, projections, rows)
	}

	plannedRows := make([]plannedRow, 0, len(rows))
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		matches, err := evaluateFilter(row, request.GetFilter())
		if err != nil {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"evaluate filter: %v",
				err,
			)
		}
		if !matches {
			continue
		}

		orderValues, err := evaluateOrderValues(
			row,
			request.GetOrderBy(),
		)
		if err != nil {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"evaluate ordering: %v",
				err,
			)
		}

		plannedRows = append(plannedRows, plannedRow{
			row:         row,
			orderValues: orderValues,
		})
	}

	if err := sortPlannedRows(plannedRows, request.GetOrderBy()); err != nil {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"sort rows: %v",
			err,
		)
	}

	plannedRows = paginateRows(
		plannedRows,
		request.Offset,
		request.Limit,
	)

	tuples := make([]*providerv1.Tuple, 0, len(plannedRows))
	for _, row := range plannedRows {
		values := make([]*kublingv1.Value, 0, len(projections))
		for _, projection := range projections {
			value, err := evaluateExpression(
				row.row,
				projection.expression,
			)
			if err != nil {
				return nil, status.Errorf(
					codes.InvalidArgument,
					"evaluate projection %q: %v",
					projection.field.GetName(),
					err,
				)
			}

			output, err := c.outputValue(
				value,
				request.GetAcceptedFeatures(),
			)
			if err != nil {
				return nil, err
			}
			values = append(values, output)
		}

		tuples = append(tuples, &providerv1.Tuple{
			Values: values,
		})
	}

	fields := make([]*providerv1.Field, 0, len(projections))
	for _, projection := range projections {
		fields = append(fields, projection.field)
	}
	querylog.Executed(ctx, c.queryLogger, request, len(tuples))

	return newResultStream(
		fields,
		tuples,
		queryBatchSize(request.BatchSize),
	), nil
}

func planProjections(
	entity *entityDefinition,
	requested []*providerv1.Projection,
) ([]plannedProjection, error) {
	if len(requested) == 0 {
		projections := make([]plannedProjection, 0, len(entity.fields))
		for _, field := range entity.fields {
			projections = append(projections, plannedProjection{
				expression: &providerv1.Expression{
					Kind: &providerv1.Expression_Field{
						Field: &providerv1.FieldReference{
							Name: field.name,
						},
					},
				},
				field: &providerv1.Field{
					Name:           field.name,
					Type:           field.valueType,
					TypeDescriptor: field.typeDescriptor,
				},
			})
		}

		return projections, nil
	}

	projections := make([]plannedProjection, 0, len(requested))
	for _, projection := range requested {
		if projection == nil {
			return nil, fmt.Errorf("projection is required")
		}

		outputName := projection.GetOutputName()
		if outputName == "" {
			if field := projection.GetExpression().GetField(); field != nil {
				outputName = field.GetName()
			}
		}
		if outputName == "" {
			return nil, fmt.Errorf("projection output_name is required")
		}

		valueType, err := expressionValueType(
			entity,
			projection.GetExpression(),
		)
		if err != nil {
			return nil, fmt.Errorf(
				"projection %q: %w",
				outputName,
				err,
			)
		}

		projections = append(projections, plannedProjection{
			expression: projection.GetExpression(),
			field: &providerv1.Field{
				Name: outputName,
				Type: valueType,
				TypeDescriptor: expressionTypeDescriptor(
					entity,
					projection.GetExpression(),
				),
			},
		})
	}

	return projections, nil
}

func expressionTypeDescriptor(
	entity *entityDefinition,
	expression *providerv1.Expression,
) *kublingv1.TypeDescriptor {
	switch kind := expression.GetKind().(type) {
	case *providerv1.Expression_Field:
		field, found := entity.fieldByName(kind.Field.GetName())
		if found {
			return field.typeDescriptor
		}
	case *providerv1.Expression_Literal:
		if declaredType := kind.Literal.GetDeclaredType(); declaredType != nil {
			return declaredType
		}
		if array := kind.Literal.GetValue().GetArrayValue(); array != nil {
			return &kublingv1.TypeDescriptor{
				Type:        kublingv1.ValueType_VALUE_TYPE_ARRAY,
				ElementType: array.GetElementType(),
			}
		}
	case *providerv1.Expression_Aggregate:
		if kind.Aggregate != nil && kind.Aggregate.GetResultType() != nil {
			return proto.Clone(kind.Aggregate.GetResultType()).(*kublingv1.TypeDescriptor)
		}
	}

	return nil
}

func (c *Connection) outputValue(
	value *kublingv1.Value,
	acceptedFeatures []string,
) (*kublingv1.Value, error) {
	if slices.Contains(acceptedFeatures, grpcfeatures.LobReadV1) {
		switch typed := value.GetKind().(type) {
		case *kublingv1.Value_BlobValue:
			return c.newLobValue(
				kublingv1.ValueType_VALUE_TYPE_BLOB,
				typed.BlobValue.GetData(),
			)
		case *kublingv1.Value_ClobValue:
			return c.newLobValue(
				kublingv1.ValueType_VALUE_TYPE_CLOB,
				[]byte(typed.ClobValue.GetData()),
			)
		}
	}

	if !slices.Contains(acceptedFeatures, grpcfeatures.SpatialValuesV1) {
		return value, nil
	}

	const sampleSRID int32 = 4326
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_GeometryValue:
		return &kublingv1.Value{Kind: &kublingv1.Value_GeometryWithCrs{
			GeometryWithCrs: &kublingv1.SpatialValue{
				Wkb:  append([]byte(nil), typed.GeometryValue...),
				Srid: pointer(sampleSRID),
			},
		}}, nil
	case *kublingv1.Value_GeographyValue:
		return &kublingv1.Value{Kind: &kublingv1.Value_GeographyWithCrs{
			GeographyWithCrs: &kublingv1.SpatialValue{
				Wkb:  append([]byte(nil), typed.GeographyValue...),
				Srid: pointer(sampleSRID),
			},
		}}, nil
	default:
		return value, nil
	}
}

func pointer[T any](value T) *T {
	return &value
}

func evaluateOrderValues(
	row entityRow,
	orderBy []*providerv1.OrderBy,
) ([]*kublingv1.Value, error) {
	values := make([]*kublingv1.Value, 0, len(orderBy))
	for _, ordering := range orderBy {
		if ordering == nil || ordering.GetExpression() == nil {
			return nil, fmt.Errorf("ordering expression is required")
		}

		value, err := evaluateExpression(
			row,
			ordering.GetExpression(),
		)
		if err != nil {
			return nil, err
		}

		values = append(values, value)
	}

	return values, nil
}

func sortPlannedRows(
	rows []plannedRow,
	orderBy []*providerv1.OrderBy,
) error {
	var compareErr error

	sort.SliceStable(rows, func(leftIndex, rightIndex int) bool {
		if compareErr != nil {
			return false
		}

		left := rows[leftIndex]
		right := rows[rightIndex]

		for index, ordering := range orderBy {
			order, err := compareOrderedValues(
				left.orderValues[index],
				right.orderValues[index],
				ordering,
			)
			if err != nil {
				compareErr = err
				return false
			}
			if order != 0 {
				return order < 0
			}
		}

		return left.row.key < right.row.key
	})

	return compareErr
}

func compareOrderedValues(
	left *kublingv1.Value,
	right *kublingv1.Value,
	ordering *providerv1.OrderBy,
) (int, error) {
	leftNull := isNullValue(left)
	rightNull := isNullValue(right)

	if leftNull || rightNull {
		if leftNull && rightNull {
			return 0, nil
		}

		nullOrdering := ordering.GetNullOrdering()
		if nullOrdering ==
			providerv1.NullOrdering_NULL_ORDERING_UNSPECIFIED {
			nullOrdering =
				providerv1.NullOrdering_NULL_ORDERING_LAST
		}

		if leftNull {
			if nullOrdering ==
				providerv1.NullOrdering_NULL_ORDERING_FIRST {
				return -1, nil
			}

			return 1, nil
		}

		if nullOrdering ==
			providerv1.NullOrdering_NULL_ORDERING_FIRST {
			return 1, nil
		}

		return -1, nil
	}

	result, err := compareValues(left, right)
	if err != nil {
		return 0, err
	}

	if ordering.GetDirection() ==
		providerv1.SortDirection_SORT_DIRECTION_DESCENDING {
		result = -result
	}

	return result, nil
}

func paginateRows(
	rows []plannedRow,
	offset *uint64,
	limit *uint64,
) []plannedRow {
	start := uint64(0)
	if offset != nil {
		start = *offset
	}
	if start >= uint64(len(rows)) {
		return nil
	}

	end := uint64(len(rows))
	if limit != nil && *limit < end-start {
		end = start + *limit
	}

	return rows[int(start):int(end)]
}

func queryBatchSize(requested *uint32) int {
	if requested == nil || *requested == 0 {
		return defaultBatchSize
	}

	return int(*requested)
}
