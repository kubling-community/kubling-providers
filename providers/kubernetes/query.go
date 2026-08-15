package kubernetes

import (
	"context"
	"fmt"
	"math"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/dynamic"
)

const defaultQueryBatchSize = 100

type resolvedResource struct {
	descriptor *resourceDescriptor
	table      *providerv1.TableMetadata
	client     kubernetesClient
	resource   dynamic.NamespaceableResourceInterface
	strategy   BlankNamespaceStrategy
}

type queryCriteria struct {
	name      *string
	namespace *string
	empty     bool
}

type queryProjection struct {
	field   *providerv1.Field
	column  *providerv1.ColumnMetadata
	literal *kublingv1.Value
}

// Query streams Kubernetes resources through the dynamic client.
func (c *Connection) Query(
	ctx context.Context,
	request *providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "query request is required")
	}
	if request.GetOffset() > 0 {
		return nil, status.Error(codes.InvalidArgument, "Kubernetes queries do not support offset")
	}
	if len(request.GetOrderBy()) > 0 {
		return nil, status.Error(codes.InvalidArgument, "Kubernetes queries do not support ordering")
	}

	resolved, err := c.resolveResource(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}
	projections, err := planQueryProjections(resolved.table, request.GetProjections())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan Kubernetes projections: %v", err)
	}
	criteria, err := planQueryCriteria(request.GetFilter())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan Kubernetes filter: %v", err)
	}

	fieldsMetadata := projectionFields(projections)
	if request.Limit != nil && request.GetLimit() == 0 || criteria.empty {
		return newEmptyKubernetesResultStream(fieldsMetadata), nil
	}

	namespace := ""
	if resolved.descriptor.resource.Namespaced {
		namespace, err = queryNamespace(
			c.provider.config.BlankNamespaceStrategy,
			resolved.client.DefaultNamespace(),
			criteria.namespace,
		)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "%v", err)
		}
	} else if criteria.namespace != nil {
		return newEmptyKubernetesResultStream(fieldsMetadata), nil
	}

	options := metav1.ListOptions{}
	if criteria.name != nil {
		options.FieldSelector = fields.OneTermEqualSelector("metadata.name", *criteria.name).String()
	}

	var resource dynamic.ResourceInterface = resolved.resource
	if resolved.descriptor.resource.Namespaced {
		resource = resolved.resource.Namespace(namespace)
	}
	return newKubernetesResultStream(
		resource,
		options,
		projections,
		queryBatchSize(request.BatchSize),
		request.Limit,
	), nil
}

func (c *Connection) resolveResource(
	ctx context.Context,
	entity *providerv1.EntityReference,
) (*resolvedResource, error) {
	if entity == nil || strings.TrimSpace(entity.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "query entity name is required")
	}
	if strings.TrimSpace(entity.GetNamespace()) == "" {
		return nil, status.Error(codes.InvalidArgument, "query entity namespace is required")
	}

	client, err := c.clientFor(ctx)
	if err != nil {
		return nil, err
	}
	if client.Discovery() == nil {
		return nil, status.Error(codes.Internal, "Kubernetes client returned nil discovery interface")
	}
	if client.Dynamic() == nil {
		return nil, status.Error(codes.Internal, "Kubernetes client returned nil dynamic interface")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	resourceLists, discoveryErr := client.Discovery().ServerPreferredResources()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	failedGroups := failedDiscoveryGroups(discoveryErr)
	if discoveryErr != nil && len(failedGroups) == 0 {
		return nil, operationError("discover Kubernetes resources", discoveryErr)
	}

	descriptors := discoverableResources(resourceLists)
	descriptors = filterResourceDescriptors(descriptors, c.provider.config.Schema)
	assignTableNames(descriptors)
	for _, descriptor := range descriptors {
		if expectedEntityNamespace(c.provider.config, descriptor.groupVersion.String()) != entity.GetNamespace() ||
			!strings.EqualFold(descriptor.tableName, entity.GetName()) {
			continue
		}

		depth := c.provider.config.Schema.expansionDepth(
			descriptor.groupVersion.String(),
			descriptor.resource.Name,
		)
		var resolver *openAPISchemaResolver
		if depth > 0 {
			resolver = newOpenAPISchemaResolver(
				ctx,
				client.Discovery(),
				&c.provider.openAPICache,
			)
		}

		table, err := configuredTableMetadata(
			resourceTableMetadataWithSchema(
				descriptor,
				resolver,
				depth,
				c.provider.config.Schema.includeObject(),
			),
			c.provider.config,
		)
		if err != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "configure Kubernetes namespace metadata: %v", err)
		}

		return &resolvedResource{
			descriptor: descriptor,
			table:      table,
			client:     client,
			strategy:   c.provider.config.BlankNamespaceStrategy,
			resource: client.Dynamic().Resource(
				descriptor.groupVersion.WithResource(descriptor.resource.Name),
			),
		}, nil
	}
	for _, failedGroup := range failedGroups {
		if failedGroup == entity.GetNamespace() {
			return nil, operationError("discover Kubernetes resource", discoveryErr)
		}
	}
	return nil, status.Errorf(
		codes.NotFound,
		"Kubernetes entity %q was not found in namespace %q",
		entity.GetName(),
		entity.GetNamespace(),
	)
}

func planQueryProjections(
	table *providerv1.TableMetadata,
	requested []*providerv1.Projection,
) ([]queryProjection, error) {
	columns := make(map[string]*providerv1.ColumnMetadata, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		columns[strings.ToUpper(column.GetName())] = column
	}
	if len(requested) == 0 {
		projections := make([]queryProjection, 0, len(table.GetColumns()))
		for _, column := range table.GetColumns() {
			projections = append(projections, queryProjection{
				field:  &providerv1.Field{Name: column.GetName(), Type: column.GetType()},
				column: column,
			})
		}
		return projections, nil
	}

	projections := make([]queryProjection, 0, len(requested))
	for _, requestedProjection := range requested {
		if requestedProjection == nil || requestedProjection.GetExpression() == nil {
			return nil, fmt.Errorf("projection expression is required")
		}
		expression := requestedProjection.GetExpression()
		outputName := strings.TrimSpace(requestedProjection.GetOutputName())
		if field := expression.GetField(); field != nil {
			column := columns[strings.ToUpper(strings.TrimSpace(field.GetName()))]
			if column == nil {
				return nil, fmt.Errorf("column %q was not found", field.GetName())
			}
			if outputName == "" {
				outputName = column.GetName()
			}
			projections = append(projections, queryProjection{
				field:  &providerv1.Field{Name: outputName, Type: column.GetType()},
				column: column,
			})
			continue
		}
		if literal := expression.GetLiteral(); literal != nil {
			if outputName == "" {
				return nil, fmt.Errorf("literal projection output_name is required")
			}
			valueType, err := inferValueType(literal.GetValue())
			if err != nil {
				return nil, err
			}
			projections = append(projections, queryProjection{
				field:   &providerv1.Field{Name: outputName, Type: valueType},
				literal: proto.Clone(literal.GetValue()).(*kublingv1.Value),
			})
			continue
		}
		return nil, fmt.Errorf("only field and literal projections are supported")
	}
	return projections, nil
}

func planQueryCriteria(expression *providerv1.Expression) (queryCriteria, error) {
	criteria := queryCriteria{}
	if expression == nil {
		return criteria, nil
	}
	if err := collectQueryCriteria(expression, &criteria); err != nil {
		return queryCriteria{}, err
	}
	return criteria, nil
}

func collectQueryCriteria(expression *providerv1.Expression, criteria *queryCriteria) error {
	if logical := expression.GetLogical(); logical != nil {
		if logical.GetOperator() != providerv1.LogicalOperator_LOGICAL_OPERATOR_AND || len(logical.GetOperands()) < 2 {
			return fmt.Errorf("only AND with at least two operands is supported")
		}
		for _, operand := range logical.GetOperands() {
			if err := collectQueryCriteria(operand, criteria); err != nil {
				return err
			}
		}
		return nil
	}

	comparison := expression.GetComparison()
	if comparison == nil || comparison.GetOperator() != providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL {
		return fmt.Errorf("only equality comparisons are supported")
	}
	field, value, err := fieldStringComparison(comparison.GetLeft(), comparison.GetRight())
	if err != nil {
		field, value, err = fieldStringComparison(comparison.GetRight(), comparison.GetLeft())
		if err != nil {
			return err
		}
	}
	switch strings.ToUpper(field) {
	case "METADATA__NAME":
		mergeQueryCriterion(&criteria.name, value, criteria)
	case "METADATA__NAMESPACE":
		mergeQueryCriterion(&criteria.namespace, value, criteria)
	default:
		return fmt.Errorf("column %q does not support Kubernetes filter pushdown", field)
	}
	return nil
}

func fieldStringComparison(
	fieldExpression *providerv1.Expression,
	literalExpression *providerv1.Expression,
) (string, string, error) {
	field := fieldExpression.GetField()
	literal := literalExpression.GetLiteral()
	if field == nil || literal == nil || literal.GetValue() == nil {
		return "", "", fmt.Errorf("comparison requires one field and one string literal")
	}
	if _, ok := literal.GetValue().GetKind().(*kublingv1.Value_StringValue); !ok {
		return "", "", fmt.Errorf("comparison literal for %q must be a string", field.GetName())
	}
	return strings.TrimSpace(field.GetName()), literal.GetValue().GetStringValue(), nil
}

func mergeQueryCriterion(target **string, value string, criteria *queryCriteria) {
	if *target != nil && **target != value {
		criteria.empty = true
		return
	}
	copyValue := value
	*target = &copyValue
}

func queryNamespace(
	strategy BlankNamespaceStrategy,
	defaultNamespace string,
	explicit *string,
) (string, error) {
	if explicit != nil {
		if strings.TrimSpace(*explicit) == "" {
			return "", fmt.Errorf("metadata__namespace must not be blank")
		}
		return *explicit, nil
	}
	switch strategy {
	case BlankNamespaceDefault:
		return defaultNamespace, nil
	case BlankNamespaceAll:
		return metav1.NamespaceAll, nil
	case BlankNamespaceFail:
		return "", fmt.Errorf("metadata__namespace criterion is required by blankNamespaceStrategy FAIL")
	default:
		return "", fmt.Errorf("unsupported blank namespace strategy %q", strategy)
	}
}

func projectionFields(projections []queryProjection) []*providerv1.Field {
	fieldsMetadata := make([]*providerv1.Field, 0, len(projections))
	for _, projection := range projections {
		fieldsMetadata = append(fieldsMetadata, projection.field)
	}
	return fieldsMetadata
}

func queryBatchSize(batchSize *uint32) int64 {
	if batchSize == nil || *batchSize == 0 {
		return defaultQueryBatchSize
	}
	if *batchSize > math.MaxInt32 {
		return math.MaxInt32
	}
	return int64(*batchSize)
}
