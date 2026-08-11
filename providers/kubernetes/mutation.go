package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

type mutationTarget struct {
	name      string
	namespace string
	resource  dynamic.ResourceInterface
}

// Insert creates Kubernetes resources from canonical object fields.
func (c *Connection) Insert(
	ctx context.Context,
	request *providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "insert request is required")
	}
	if len(request.GetReturningFields()) > 0 {
		return nil, status.Error(codes.InvalidArgument, "Kubernetes inserts do not return generated values")
	}
	resolved, err := c.resolveResource(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}
	if !hasVerb(resolved.descriptor.resource.Verbs, "create") {
		return nil, mutationVerbError(resolved.table, "create")
	}
	rows := request.GetRows()
	if rows == nil || len(rows.GetFields()) == 0 || len(rows.GetTuples()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "insert fields and tuples are required")
	}
	columns, err := mutationColumns(resolved.table, rows.GetFields())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "resolve Kubernetes insert fields: %v", err)
	}

	var affected uint64
	for rowIndex, tuple := range rows.GetTuples() {
		if tuple == nil || len(tuple.GetValues()) != len(columns) {
			return nil, status.Errorf(codes.InvalidArgument, "insert tuple %d has %d values, want %d", rowIndex, len(tuple.GetValues()), len(columns))
		}
		object, err := buildInsertObject(resolved, columns, tuple.GetValues())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "build Kubernetes insert tuple %d: %v", rowIndex, err)
		}
		resource, err := mutationResource(resolved, object.GetNamespace())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "build Kubernetes insert tuple %d: %v", rowIndex, err)
		}
		created, err := resource.Create(ctx, object, metav1.CreateOptions{})
		if err != nil {
			return nil, kubernetesOperationError("create Kubernetes resource", err)
		}
		if created == nil {
			return nil, status.Error(codes.Internal, "Kubernetes dynamic client returned nil created resource")
		}
		affected++
	}
	return &providerv1.InsertResponse{AffectedRows: &affected}, nil
}

// Update applies JSON assignments to one Kubernetes resource identified by its key.
func (c *Connection) Update(
	ctx context.Context,
	request *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if request == nil || request.GetFilter() == nil {
		return nil, status.Error(codes.InvalidArgument, "Kubernetes update requires a filter")
	}
	resolved, err := c.resolveResource(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}
	canPatch := hasVerb(resolved.descriptor.resource.Verbs, "patch")
	canUpdate := hasVerb(resolved.descriptor.resource.Verbs, "update")
	if !canPatch && !canUpdate {
		return nil, mutationVerbError(resolved.table, "update or patch")
	}
	target, err := resolveMutationTarget(resolved, request.GetFilter())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan Kubernetes update filter: %v", err)
	}
	if target == nil {
		return &providerv1.UpdateResponse{AffectedRows: affectedRows(0)}, nil
	}
	assignments, err := planMutationAssignments(resolved.table, request.GetAssignments())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan Kubernetes assignments: %v", err)
	}

	if canPatch && (!canUpdate || !hasObjectAssignment(assignments)) {
		patch, err := buildMergePatch(resolved, target, assignments)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "build Kubernetes merge patch: %v", err)
		}
		updated, err := target.resource.Patch(ctx, target.name, types.MergePatchType, patch, metav1.PatchOptions{})
		if apierrors.IsNotFound(err) {
			return &providerv1.UpdateResponse{AffectedRows: affectedRows(0)}, nil
		}
		if err != nil {
			return nil, kubernetesOperationError("patch Kubernetes resource", err)
		}
		if updated == nil {
			return nil, status.Error(codes.Internal, "Kubernetes dynamic client returned nil patched resource")
		}
		return &providerv1.UpdateResponse{AffectedRows: affectedRows(1)}, nil
	}

	object, err := buildUpdatedObject(ctx, resolved, target, assignments)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return &providerv1.UpdateResponse{AffectedRows: affectedRows(0)}, nil
		}
		return nil, err
	}
	updated, err := target.resource.Update(ctx, object, metav1.UpdateOptions{})
	if apierrors.IsNotFound(err) {
		return &providerv1.UpdateResponse{AffectedRows: affectedRows(0)}, nil
	}
	if err != nil {
		return nil, kubernetesOperationError("update Kubernetes resource", err)
	}
	if updated == nil {
		return nil, status.Error(codes.Internal, "Kubernetes dynamic client returned nil updated resource")
	}
	return &providerv1.UpdateResponse{AffectedRows: affectedRows(1)}, nil
}

// Delete removes one Kubernetes resource identified by its key.
func (c *Connection) Delete(
	ctx context.Context,
	request *providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if request == nil || request.GetFilter() == nil {
		return nil, status.Error(codes.InvalidArgument, "Kubernetes delete requires a filter")
	}
	resolved, err := c.resolveResource(ctx, request.GetEntity())
	if err != nil {
		return nil, err
	}
	if !hasVerb(resolved.descriptor.resource.Verbs, "delete") {
		return nil, mutationVerbError(resolved.table, "delete")
	}
	target, err := resolveMutationTarget(resolved, request.GetFilter())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan Kubernetes delete filter: %v", err)
	}
	if target == nil {
		return &providerv1.DeleteResponse{AffectedRows: affectedRows(0)}, nil
	}
	if err := target.resource.Delete(ctx, target.name, metav1.DeleteOptions{}); err != nil {
		if apierrors.IsNotFound(err) {
			return &providerv1.DeleteResponse{AffectedRows: affectedRows(0)}, nil
		}
		return nil, kubernetesOperationError("delete Kubernetes resource", err)
	}
	return &providerv1.DeleteResponse{AffectedRows: affectedRows(1)}, nil
}

func mutationColumns(table *providerv1.TableMetadata, fields []*providerv1.Field) ([]*providerv1.ColumnMetadata, error) {
	available := make(map[string]*providerv1.ColumnMetadata, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		available[strings.ToUpper(column.GetName())] = column
	}
	columns := make([]*providerv1.ColumnMetadata, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field == nil || strings.TrimSpace(field.GetName()) == "" {
			return nil, fmt.Errorf("field name is required")
		}
		key := strings.ToUpper(strings.TrimSpace(field.GetName()))
		column := available[key]
		if column == nil {
			return nil, fmt.Errorf("column %q was not found", field.GetName())
		}
		if !column.GetUpdatable() {
			return nil, fmt.Errorf("column %q is read-only", column.GetName())
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("column %q is repeated", column.GetName())
		}
		seen[key] = struct{}{}
		columns = append(columns, column)
	}
	if err := validateObjectMutationColumns(columns); err != nil {
		return nil, err
	}
	return columns, nil
}

func buildInsertObject(
	resolved *resolvedResource,
	columns []*providerv1.ColumnMetadata,
	values []*kublingv1.Value,
) (*unstructured.Unstructured, error) {
	object := make(map[string]any)
	for index, column := range columns {
		if !strings.EqualFold(column.GetName(), "object") {
			continue
		}
		parsed, err := jsonObject(values[index])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", column.GetName(), err)
		}
		object = parsed
	}
	resource := &unstructured.Unstructured{Object: object}
	for index, column := range columns {
		name := strings.ToUpper(column.GetName())
		switch name {
		case "METADATA":
			value, err := jsonObject(values[index])
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", column.GetName(), err)
			}
			resource.Object["metadata"] = value
		case "SPEC":
			value, err := jsonDocument(values[index])
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", column.GetName(), err)
			}
			resource.Object["spec"] = value
		}
	}
	for index, column := range columns {
		switch strings.ToUpper(column.GetName()) {
		case "OBJECT", "METADATA", "SPEC", "API_VERSION", "KIND", "METADATA__NAME", "METADATA__NAMESPACE":
			continue
		default:
			path, err := mutationColumnPath(column)
			if err != nil || !kubernetesFieldPathWritable(path) {
				return nil, fmt.Errorf("column %q is not supported for Kubernetes inserts", column.GetName())
			}
			value, err := kubernetesMutationValue(values[index], column.GetType())
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", column.GetName(), err)
			}
			if err := unstructured.SetNestedField(resource.Object, value, path...); err != nil {
				return nil, fmt.Errorf("column %q: set %s: %w", column.GetName(), strings.Join(path, "."), err)
			}
		}
	}
	for index, column := range columns {
		switch strings.ToUpper(column.GetName()) {
		case "API_VERSION":
			value, err := requiredString(values[index])
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", column.GetName(), err)
			}
			resource.SetAPIVersion(value)
		case "KIND":
			value, err := requiredString(values[index])
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", column.GetName(), err)
			}
			resource.SetKind(value)
		case "METADATA__NAME":
			value, err := requiredString(values[index])
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", column.GetName(), err)
			}
			resource.SetName(value)
		case "METADATA__NAMESPACE":
			value, err := optionalString(values[index])
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", column.GetName(), err)
			}
			resource.SetNamespace(value)
		}
	}
	if err := normalizeResourceIdentity(resolved, resource, ""); err != nil {
		return nil, err
	}
	if resource.GetName() == "" && resource.GetGenerateName() == "" {
		return nil, fmt.Errorf("metadata.name or metadata.generateName is required")
	}
	return resource, nil
}

func resolveMutationTarget(
	resolved *resolvedResource,
	filter *providerv1.Expression,
) (*mutationTarget, error) {
	criteria, err := planQueryCriteria(filter)
	if err != nil {
		return nil, err
	}
	if criteria.empty {
		return nil, nil
	}
	if criteria.name == nil || strings.TrimSpace(*criteria.name) == "" {
		return nil, fmt.Errorf("metadata__name equality criterion is required")
	}
	namespace := ""
	if resolved.descriptor.resource.Namespaced {
		namespace, err = mutationNamespace(resolved.strategy, resolved.client.DefaultNamespace(), criteria.namespace)
		if err != nil {
			return nil, err
		}
	} else if criteria.namespace != nil {
		return nil, nil
	}
	resource, err := mutationResource(resolved, namespace)
	if err != nil {
		return nil, err
	}
	return &mutationTarget{name: *criteria.name, namespace: namespace, resource: resource}, nil
}

func mutationNamespace(
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
		if strings.TrimSpace(defaultNamespace) == "" {
			return "", fmt.Errorf("default Kubernetes namespace is unavailable")
		}
		return defaultNamespace, nil
	case BlankNamespaceAll, BlankNamespaceFail:
		return "", fmt.Errorf("metadata__namespace criterion is required by blankNamespaceStrategy %s", strategy)
	default:
		return "", fmt.Errorf("unsupported blank namespace strategy %q", strategy)
	}
}

func mutationResource(resolved *resolvedResource, namespace string) (dynamic.ResourceInterface, error) {
	if resolved.descriptor.resource.Namespaced {
		if strings.TrimSpace(namespace) == "" {
			return nil, fmt.Errorf("metadata.namespace is required for namespaced resources")
		}
		return resolved.resource.Namespace(namespace), nil
	}
	if strings.TrimSpace(namespace) != "" {
		return nil, fmt.Errorf("cluster-scoped resources cannot declare metadata.namespace")
	}
	return resolved.resource, nil
}

type plannedAssignment struct {
	column *providerv1.ColumnMetadata
	path   []string
	value  any
}

func planMutationAssignments(
	table *providerv1.TableMetadata,
	assignments []*providerv1.Assignment,
) ([]plannedAssignment, error) {
	if len(assignments) == 0 {
		return nil, fmt.Errorf("at least one assignment is required")
	}
	columns := make(map[string]*providerv1.ColumnMetadata, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		columns[strings.ToUpper(column.GetName())] = column
	}
	planned := make([]plannedAssignment, 0, len(assignments))
	seen := make(map[string]struct{}, len(assignments))
	for _, assignment := range assignments {
		if assignment == nil || strings.TrimSpace(assignment.GetField()) == "" {
			return nil, fmt.Errorf("assignment field is required")
		}
		key := strings.ToUpper(strings.TrimSpace(assignment.GetField()))
		column := columns[key]
		if column == nil {
			return nil, fmt.Errorf("column %q was not found", assignment.GetField())
		}
		if !column.GetUpdatable() {
			return nil, fmt.Errorf("column %q is not updatable", column.GetName())
		}
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("column %q is assigned more than once", column.GetName())
		}
		seen[key] = struct{}{}
		expression := assignment.GetValue()
		if expression == nil {
			return nil, fmt.Errorf("assignment %q requires a literal", column.GetName())
		}
		literal := expression.GetLiteral()
		if literal == nil {
			return nil, fmt.Errorf("assignment %q requires a literal", column.GetName())
		}

		var (
			path  []string
			value any
			err   error
		)
		switch key {
		case "OBJECT":
			value, err = jsonDocument(literal.GetValue())
		case "METADATA":
			path = []string{"metadata"}
			value, err = jsonDocument(literal.GetValue())
		case "SPEC":
			path = []string{"spec"}
			value, err = jsonDocument(literal.GetValue())
		default:
			path, err = mutationColumnPath(column)
			if err == nil && !kubernetesFieldPathWritable(path) {
				err = fmt.Errorf("column is not writable by Kubernetes resource updates")
			}
			if err == nil {
				value, err = kubernetesMutationValue(literal.GetValue(), column.GetType())
			}
		}
		if err != nil {
			return nil, fmt.Errorf("assignment %q: %w", column.GetName(), err)
		}
		if key == "OBJECT" {
			if _, ok := value.(map[string]any); !ok {
				return nil, fmt.Errorf("assignment %q requires a JSON object", column.GetName())
			}
		}
		if key == "METADATA" {
			if _, ok := value.(map[string]any); !ok {
				return nil, fmt.Errorf("assignment %q requires a JSON object", column.GetName())
			}
		}
		planned = append(planned, plannedAssignment{column: column, path: path, value: value})
	}
	columnsForConflictCheck := make([]*providerv1.ColumnMetadata, 0, len(planned))
	for _, assignment := range planned {
		columnsForConflictCheck = append(columnsForConflictCheck, assignment.column)
	}
	if err := validateObjectMutationColumns(columnsForConflictCheck); err != nil {
		return nil, err
	}
	return planned, nil
}

func buildMergePatch(
	resolved *resolvedResource,
	target *mutationTarget,
	assignments []plannedAssignment,
) ([]byte, error) {
	patch := make(map[string]any)
	for _, assignment := range assignments {
		if plannedObjectAssignment(assignment) {
			for key, value := range assignment.value.(map[string]any) {
				patch[key] = value
			}
		}
	}
	if err := applyPlannedPaths(patch, assignments); err != nil {
		return nil, err
	}
	resource := &unstructured.Unstructured{Object: patch}
	if err := validateResourceIdentity(resolved, resource, target.name, target.namespace); err != nil {
		return nil, err
	}
	return json.Marshal(patch)
}

func hasObjectAssignment(assignments []plannedAssignment) bool {
	for _, assignment := range assignments {
		if plannedObjectAssignment(assignment) {
			return true
		}
	}
	return false
}

func buildUpdatedObject(
	ctx context.Context,
	resolved *resolvedResource,
	target *mutationTarget,
	assignments []plannedAssignment,
) (*unstructured.Unstructured, error) {
	var object *unstructured.Unstructured
	for _, assignment := range assignments {
		if plannedObjectAssignment(assignment) {
			object = &unstructured.Unstructured{Object: assignment.value.(map[string]any)}
			break
		}
	}
	if object == nil || object.GetResourceVersion() == "" {
		current, err := target.resource.Get(ctx, target.name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if current == nil {
			return nil, status.Error(codes.Internal, "Kubernetes dynamic client returned nil current resource")
		}
		if object == nil {
			object = current.DeepCopy()
		} else {
			object.SetResourceVersion(current.GetResourceVersion())
		}
	}
	for _, assignment := range assignments {
		if len(assignment.path) != 1 {
			continue
		}
		switch assignment.path[0] {
		case "metadata":
			metadata := make(map[string]any)
			if current, ok := object.Object["metadata"].(map[string]any); ok {
				for key, value := range current {
					metadata[key] = value
				}
			}
			for key, value := range assignment.value.(map[string]any) {
				metadata[key] = value
			}
			object.Object["metadata"] = metadata
		case "spec":
			object.Object["spec"] = assignment.value
		}
	}
	for _, assignment := range assignments {
		if len(assignment.path) <= 1 {
			continue
		}
		if err := unstructured.SetNestedField(object.Object, assignment.value, assignment.path...); err != nil {
			return nil, fmt.Errorf("set assignment %q at %s: %w", assignment.column.GetName(), strings.Join(assignment.path, "."), err)
		}
	}
	if object.GetName() == "" {
		object.SetName(target.name)
	}
	if err := normalizeResourceIdentity(resolved, object, target.namespace); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	if object.GetName() != target.name {
		return nil, status.Errorf(codes.InvalidArgument, "object metadata.name %q does not match filter name %q", object.GetName(), target.name)
	}
	return object, nil
}

func plannedObjectAssignment(assignment plannedAssignment) bool {
	return assignment.column != nil && strings.EqualFold(assignment.column.GetSourceName(), "$")
}

func applyPlannedPaths(object map[string]any, assignments []plannedAssignment) error {
	for _, assignment := range assignments {
		if len(assignment.path) != 1 {
			continue
		}
		object[assignment.path[0]] = assignment.value
	}
	for _, assignment := range assignments {
		if len(assignment.path) <= 1 {
			continue
		}
		if err := unstructured.SetNestedField(object, assignment.value, assignment.path...); err != nil {
			return fmt.Errorf("set assignment %q at %s: %w", assignment.column.GetName(), strings.Join(assignment.path, "."), err)
		}
	}
	return nil
}

func validateObjectMutationColumns(columns []*providerv1.ColumnMetadata) error {
	hasObject := false
	for _, column := range columns {
		if column != nil && strings.EqualFold(strings.TrimSpace(column.GetSourceName()), "$") {
			hasObject = true
			break
		}
	}
	if !hasObject {
		return nil
	}

	for _, column := range columns {
		if column == nil {
			continue
		}
		sourceName := strings.TrimSpace(column.GetSourceName())
		if sourceName == "$" || objectMutationIdentitySource(sourceName) {
			continue
		}
		return fmt.Errorf(
			"column %q cannot be combined with object in the same Kubernetes mutation",
			column.GetName(),
		)
	}
	return nil
}

func objectMutationIdentitySource(sourceName string) bool {
	switch sourceName {
	case "apiVersion", "kind", "metadata.name", "metadata.namespace":
		return true
	default:
		return false
	}
}

func mutationColumnPath(column *providerv1.ColumnMetadata) ([]string, error) {
	if column == nil {
		return nil, fmt.Errorf("column is required")
	}
	sourceName := strings.TrimSpace(column.GetSourceName())
	if sourceName == "" {
		sourceName = strings.ReplaceAll(column.GetName(), "__", ".")
	}
	if sourceName == "" || sourceName == "$" {
		return nil, fmt.Errorf("column source path is unavailable")
	}
	path := strings.Split(sourceName, ".")
	for _, part := range path {
		if strings.TrimSpace(part) == "" {
			return nil, fmt.Errorf("column source path %q is invalid", sourceName)
		}
	}
	return path, nil
}

func kubernetesFieldPathWritable(path []string) bool {
	if len(path) == 0 {
		return false
	}

	switch path[0] {
	case "apiVersion", "kind", "status":
		return false
	case "metadata":
		if len(path) == 1 {
			return true
		}

		switch path[1] {
		case "uid",
			"resourceVersion",
			"generation",
			"creationTimestamp",
			"deletionTimestamp",
			"deletionGracePeriodSeconds",
			"managedFields",
			"selfLink",
			"name",
			"namespace":
			return false
		default:
			return true
		}
	default:
		return true
	}
}

func kubernetesMutationValue(value *kublingv1.Value, valueType kublingv1.ValueType) (any, error) {
	if value == nil {
		return nil, fmt.Errorf("value is required")
	}
	if _, ok := value.GetKind().(*kublingv1.Value_NullValue); ok {
		return nil, nil
	}
	switch valueType {
	case kublingv1.ValueType_VALUE_TYPE_STRING:
		if _, ok := value.GetKind().(*kublingv1.Value_StringValue); !ok {
			return nil, fmt.Errorf("string value is required")
		}
		return value.GetStringValue(), nil
	case kublingv1.ValueType_VALUE_TYPE_BOOLEAN:
		if _, ok := value.GetKind().(*kublingv1.Value_BooleanValue); !ok {
			return nil, fmt.Errorf("boolean value is required")
		}
		return value.GetBooleanValue(), nil
	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		if _, ok := value.GetKind().(*kublingv1.Value_IntegerValue); !ok {
			return nil, fmt.Errorf("integer value is required")
		}
		return int64(value.GetIntegerValue()), nil
	case kublingv1.ValueType_VALUE_TYPE_LONG:
		if _, ok := value.GetKind().(*kublingv1.Value_LongValue); !ok {
			return nil, fmt.Errorf("long value is required")
		}
		return value.GetLongValue(), nil
	case kublingv1.ValueType_VALUE_TYPE_FLOAT:
		if _, ok := value.GetKind().(*kublingv1.Value_FloatValue); !ok {
			return nil, fmt.Errorf("float value is required")
		}
		return float64(value.GetFloatValue()), nil
	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		if _, ok := value.GetKind().(*kublingv1.Value_DoubleValue); !ok {
			return nil, fmt.Errorf("double value is required")
		}
		return value.GetDoubleValue(), nil
	case kublingv1.ValueType_VALUE_TYPE_JSON:
		return jsonDocument(value)
	default:
		return nil, fmt.Errorf("unsupported Kubernetes mutation type %s", valueType)
	}
}

func normalizeResourceIdentity(
	resolved *resolvedResource,
	resource *unstructured.Unstructured,
	forcedNamespace string,
) error {
	expectedAPIVersion := resolved.descriptor.groupVersion.String()
	if actual := resource.GetAPIVersion(); actual != "" && actual != expectedAPIVersion {
		return fmt.Errorf("apiVersion %q does not match %q", actual, expectedAPIVersion)
	}
	resource.SetAPIVersion(expectedAPIVersion)
	if actual := resource.GetKind(); actual != "" && actual != resolved.descriptor.resource.Kind {
		return fmt.Errorf("kind %q does not match %q", actual, resolved.descriptor.resource.Kind)
	}
	resource.SetKind(resolved.descriptor.resource.Kind)
	if resolved.descriptor.resource.Namespaced {
		namespace := resource.GetNamespace()
		if forcedNamespace != "" && namespace != "" && namespace != forcedNamespace {
			return fmt.Errorf("metadata.namespace %q does not match %q", namespace, forcedNamespace)
		}
		if namespace == "" {
			var err error
			explicit := (*string)(nil)
			if forcedNamespace != "" {
				explicit = &forcedNamespace
			}
			namespace, err = mutationNamespace(resolved.strategy, resolved.client.DefaultNamespace(), explicit)
			if err != nil {
				return err
			}
		}
		if namespace == "" {
			return fmt.Errorf("metadata.namespace is required for namespaced resources")
		}
		resource.SetNamespace(namespace)
		return nil
	}
	if resource.GetNamespace() != "" || forcedNamespace != "" {
		return fmt.Errorf("cluster-scoped resources cannot declare metadata.namespace")
	}
	return nil
}

func validateResourceIdentity(
	resolved *resolvedResource,
	resource *unstructured.Unstructured,
	name string,
	namespace string,
) error {
	if actual := resource.GetAPIVersion(); actual != "" && actual != resolved.descriptor.groupVersion.String() {
		return fmt.Errorf("apiVersion %q does not match %q", actual, resolved.descriptor.groupVersion.String())
	}
	if actual := resource.GetKind(); actual != "" && actual != resolved.descriptor.resource.Kind {
		return fmt.Errorf("kind %q does not match %q", actual, resolved.descriptor.resource.Kind)
	}
	if actual := resource.GetName(); actual != "" && actual != name {
		return fmt.Errorf("object metadata.name %q does not match filter name %q", actual, name)
	}
	if actual := resource.GetNamespace(); actual != "" && actual != namespace {
		return fmt.Errorf("object metadata.namespace %q does not match filter namespace %q", actual, namespace)
	}
	return nil
}

func jsonObject(value *kublingv1.Value) (map[string]any, error) {
	parsed, err := jsonDocument(value)
	if err != nil {
		return nil, err
	}
	object, ok := parsed.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("JSON object is required")
	}
	return object, nil
}

func jsonDocument(value *kublingv1.Value) (any, error) {
	if value == nil {
		return nil, fmt.Errorf("value is required")
	}
	switch value.GetKind().(type) {
	case *kublingv1.Value_NullValue:
		return nil, nil
	case *kublingv1.Value_JsonValue:
		var decoded any
		if err := json.Unmarshal([]byte(value.GetJsonValue()), &decoded); err != nil {
			return nil, fmt.Errorf("decode JSON: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("JSON value is required")
	}
}

func requiredString(value *kublingv1.Value) (string, error) {
	if value == nil {
		return "", fmt.Errorf("string value is required")
	}
	if _, ok := value.GetKind().(*kublingv1.Value_StringValue); !ok {
		return "", fmt.Errorf("string value is required")
	}
	if strings.TrimSpace(value.GetStringValue()) == "" {
		return "", fmt.Errorf("string value must not be blank")
	}
	return value.GetStringValue(), nil
}

func optionalString(value *kublingv1.Value) (string, error) {
	if value == nil {
		return "", fmt.Errorf("string or null value is required")
	}
	if _, ok := value.GetKind().(*kublingv1.Value_NullValue); ok {
		return "", nil
	}
	return requiredString(value)
}

func mutationVerbError(table *providerv1.TableMetadata, verb string) error {
	return status.Errorf(codes.FailedPrecondition, "Kubernetes entity %q does not support %s", table.GetName(), verb)
}

func affectedRows(value uint64) *uint64 {
	return &value
}
