// Package mutation translates bounded relational updates into BACnet writes.
package mutation

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/config"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/discovery"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	bacnet "github.com/otfabric/go-bacnet"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const maximumInstance = int32(4_194_303)

var identityColumns = []string{
	catalog.NamespaceColumn,
	catalog.ScopeIDColumn,
	catalog.DeviceInstanceColumn,
	catalog.ObjectTypeColumn,
	catalog.ObjectInstanceColumn,
}

// DeviceSource is the stable registry plus the narrow physical write surface
// needed by one-object updates.
type DeviceSource interface {
	Snapshot() discovery.Snapshot
	MutationPolicy(namespace string, scopeID string) (config.MutationPolicy, bool)
	WriteProperty(
		context.Context,
		discovery.Device,
		bacnet.ObjectIdentifier,
		bacnet.PropertyReference,
		bacnet.ApplicationValue,
		*uint8,
	) error
}

type updatePlan struct {
	column       profile.Column
	device       discovery.Device
	object       bacnet.ObjectIdentifier
	value        bacnet.ApplicationValue
	priority     *uint8
	deviceExists bool
}

// Execute validates and performs exactly one BACnet property write.
func Execute(
	ctx context.Context,
	compiled *catalog.Catalog,
	devices DeviceSource,
	request *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if devices == nil {
		return nil, status.Error(codes.FailedPrecondition, "BACnet device registry is unavailable")
	}
	planned, err := buildPlan(compiled, devices, request)
	if err != nil {
		return nil, err
	}
	if !planned.deviceExists {
		return affectedRows(0), nil
	}
	if err := devices.WriteProperty(
		ctx,
		planned.device,
		planned.object,
		bacnet.PropertyReference{Identifier: bacnet.PropertyIdentifier(planned.column.Property.ID)},
		planned.value,
		planned.priority,
	); err != nil {
		return nil, writeError(ctx, err)
	}
	return affectedRows(1), nil
}

func buildPlan(
	compiled *catalog.Catalog,
	devices DeviceSource,
	request *providerv1.UpdateRequest,
) (updatePlan, error) {
	if compiled == nil {
		return updatePlan{}, status.Error(codes.FailedPrecondition, "BACnet catalog is unavailable")
	}
	if request == nil {
		return updatePlan{}, invalid("update request is required")
	}
	entity := request.GetEntity()
	if entity == nil || strings.TrimSpace(entity.GetName()) == "" {
		return updatePlan{}, invalid("update entity is required")
	}
	if strings.TrimSpace(entity.GetNamespace()) != "" {
		return updatePlan{}, invalid("BACnet tables do not use provider entity namespaces")
	}
	if len(request.GetAssignments()) != 1 {
		return updatePlan{}, invalid("BACnet UPDATE requires exactly one assignment")
	}

	tableName := strings.ToUpper(strings.TrimSpace(entity.GetName()))
	metadata := findTableMetadata(compiled, tableName)
	if metadata == nil {
		return updatePlan{}, invalid("unknown BACnet table %q", entity.GetName())
	}
	if !metadata.GetUpdatable() {
		return updatePlan{}, invalid("BACnet table %q is read-only", metadata.GetName())
	}

	constraints, err := parseIdentityFilter(request.GetFilter())
	if err != nil {
		return updatePlan{}, invalid("BACnet UPDATE filter: %v", err)
	}
	namespace, err := requiredString(constraints, catalog.NamespaceColumn)
	if err != nil {
		return updatePlan{}, invalid("BACnet UPDATE filter: %v", err)
	}
	scopeID, err := requiredString(constraints, catalog.ScopeIDColumn)
	if err != nil {
		return updatePlan{}, invalid("BACnet UPDATE filter: %v", err)
	}
	deviceInstance, err := requiredInstance(constraints, catalog.DeviceInstanceColumn)
	if err != nil {
		return updatePlan{}, invalid("BACnet UPDATE filter: %v", err)
	}
	objectTypeName, err := requiredString(constraints, catalog.ObjectTypeColumn)
	if err != nil {
		return updatePlan{}, invalid("BACnet UPDATE filter: %v", err)
	}
	objectInstance, err := requiredInstance(constraints, catalog.ObjectInstanceColumn)
	if err != nil {
		return updatePlan{}, invalid("BACnet UPDATE filter: %v", err)
	}

	policy, exists := devices.MutationPolicy(namespace, scopeID)
	if !exists {
		return updatePlan{}, status.Errorf(
			codes.FailedPrecondition,
			"BACnet scope %s/%s is not configured",
			namespace,
			scopeID,
		)
	}
	if !policy.Enabled {
		return updatePlan{}, status.Errorf(
			codes.FailedPrecondition,
			"BACnet mutations are disabled for scope %s/%s",
			namespace,
			scopeID,
		)
	}

	device, exists := findDevice(devices.Snapshot(), namespace, scopeID, uint32(deviceInstance))
	if !exists {
		return updatePlan{deviceExists: false}, nil
	}
	selected, exists := compiled.Profiles().Profile(device.ProfileID)
	if !exists || selected == nil || selected.Abstract {
		return updatePlan{}, status.Errorf(
			codes.FailedPrecondition,
			"registered BACnet device %s/%s/%d has no usable profile",
			namespace,
			scopeID,
			deviceInstance,
		)
	}
	declared, exists := findProfileTable(selected, tableName)
	if !exists {
		return updatePlan{}, invalid(
			"BACnet table %q is not available for device profile %q",
			tableName,
			selected.ID,
		)
	}
	objectType, exists := findObjectType(selected, objectTypeName)
	if !exists || !tableContainsObjectType(declared, objectType.Name) {
		return updatePlan{}, invalid(
			"object type %q is not exposed by BACnet table %q for profile %q",
			objectTypeName,
			tableName,
			selected.ID,
		)
	}

	assignment := request.GetAssignments()[0]
	field := strings.ToLower(strings.TrimSpace(assignment.GetField()))
	if field == "" {
		return updatePlan{}, invalid("BACnet UPDATE assignment field is required")
	}
	column, exists := findColumn(declared, field)
	if !exists {
		return updatePlan{}, invalid("unknown %s column %q", tableName, assignment.GetField())
	}
	if !column.WritableFor(objectType.Name) {
		return updatePlan{}, invalid(
			"column %q is not writable for BACnet object type %q",
			column.Name,
			objectType.Name,
		)
	}
	literal := assignment.GetValue().GetLiteral()
	if literal == nil || literal.GetValue() == nil || literal.GetValue().GetKind() == nil {
		return updatePlan{}, invalid("BACnet UPDATE assignment must be a literal")
	}
	value, relinquish, err := applicationValue(column, literal.GetValue())
	if err != nil {
		return updatePlan{}, invalid("assignment for column %q: %v", column.Name, err)
	}
	if relinquish && !column.Write.Commandable {
		return updatePlan{}, invalid("NULL is allowed only for commandable BACnet properties")
	}
	var priority *uint8
	if column.Write.Commandable {
		copied := policy.Priority
		priority = &copied
	}
	return updatePlan{
		column:       column,
		device:       device,
		object:       bacnet.ObjectIdentifier{Type: bacnet.ObjectType(objectType.ID), Instance: uint32(objectInstance)},
		value:        value,
		priority:     priority,
		deviceExists: true,
	}, nil
}

func parseIdentityFilter(expression *providerv1.Expression) (map[string]*kublingv1.Value, error) {
	if expression == nil {
		return nil, fmt.Errorf("all identity columns are required")
	}
	terms, err := equalityTerms(expression)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(identityColumns))
	for _, name := range identityColumns {
		allowed[name] = struct{}{}
	}
	result := make(map[string]*kublingv1.Value, len(identityColumns))
	for _, term := range terms {
		name := strings.ToLower(strings.TrimSpace(term.field))
		if _, exists := allowed[name]; !exists {
			return nil, fmt.Errorf("column %q is not part of the exact object identity", term.field)
		}
		if previous := result[name]; previous != nil && !proto.Equal(previous, term.value) {
			return nil, fmt.Errorf("column %q has conflicting equality values", term.field)
		}
		result[name] = term.value
	}
	for _, name := range identityColumns {
		if result[name] == nil {
			return nil, fmt.Errorf("missing equality predicate for %s", name)
		}
	}
	return result, nil
}

type equalityTerm struct {
	field string
	value *kublingv1.Value
}

func equalityTerms(expression *providerv1.Expression) ([]equalityTerm, error) {
	if expression == nil {
		return nil, fmt.Errorf("filter expression is required")
	}
	switch kind := expression.GetKind().(type) {
	case *providerv1.Expression_Comparison:
		if kind.Comparison == nil ||
			kind.Comparison.GetOperator() != providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL {
			return nil, fmt.Errorf("only equality predicates are supported")
		}
		field, value, ok := fieldLiteral(kind.Comparison.GetLeft(), kind.Comparison.GetRight())
		if !ok {
			field, value, ok = fieldLiteral(kind.Comparison.GetRight(), kind.Comparison.GetLeft())
		}
		if !ok {
			return nil, fmt.Errorf("each predicate must compare a field with a non-null literal")
		}
		return []equalityTerm{{field: field, value: value}}, nil
	case *providerv1.Expression_Logical:
		if kind.Logical == nil ||
			kind.Logical.GetOperator() != providerv1.LogicalOperator_LOGICAL_OPERATOR_AND ||
			len(kind.Logical.GetOperands()) < 2 {
			return nil, fmt.Errorf("only AND combinations of equality predicates are supported")
		}
		var result []equalityTerm
		for _, operand := range kind.Logical.GetOperands() {
			terms, err := equalityTerms(operand)
			if err != nil {
				return nil, err
			}
			result = append(result, terms...)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported BACnet UPDATE filter expression")
	}
}

func fieldLiteral(
	fieldExpression *providerv1.Expression,
	literalExpression *providerv1.Expression,
) (string, *kublingv1.Value, bool) {
	if fieldExpression == nil || literalExpression == nil {
		return "", nil, false
	}
	field := fieldExpression.GetField()
	literal := literalExpression.GetLiteral()
	if field == nil || strings.TrimSpace(field.GetName()) == "" ||
		literal == nil || literal.GetValue() == nil || literal.GetValue().GetKind() == nil {
		return "", nil, false
	}
	if _, null := literal.GetValue().GetKind().(*kublingv1.Value_NullValue); null {
		return "", nil, false
	}
	return field.GetName(), literal.GetValue(), true
}

func requiredString(values map[string]*kublingv1.Value, name string) (string, error) {
	typed, ok := values[name].GetKind().(*kublingv1.Value_StringValue)
	if !ok || strings.TrimSpace(typed.StringValue) == "" {
		return "", fmt.Errorf("%s must be a non-empty STRING literal", name)
	}
	return typed.StringValue, nil
}

func requiredInstance(values map[string]*kublingv1.Value, name string) (int32, error) {
	typed, ok := values[name].GetKind().(*kublingv1.Value_IntegerValue)
	if !ok {
		return 0, fmt.Errorf("%s must be an INTEGER literal", name)
	}
	if typed.IntegerValue < 0 || typed.IntegerValue > maximumInstance {
		return 0, fmt.Errorf("%s must be between 0 and %d", name, maximumInstance)
	}
	return typed.IntegerValue, nil
}

func applicationValue(
	column profile.Column,
	value *kublingv1.Value,
) (bacnet.ApplicationValue, bool, error) {
	if _, null := value.GetKind().(*kublingv1.Value_NullValue); null {
		return bacnet.NullValue(), true, nil
	}
	switch column.BACnetType {
	case profile.BACnetBoolean:
		typed, ok := value.GetKind().(*kublingv1.Value_BooleanValue)
		if !ok {
			return bacnet.ApplicationValue{}, false, fmt.Errorf("expected BOOLEAN literal")
		}
		return bacnet.BoolValue(typed.BooleanValue), false, nil
	case profile.BACnetUnsigned:
		number, err := integerValue(column, value)
		if err != nil {
			return bacnet.ApplicationValue{}, false, err
		}
		if number.Sign() < 0 || !number.IsUint64() {
			return bacnet.ApplicationValue{}, false, fmt.Errorf("unsigned value is out of range")
		}
		return bacnet.UnsignedValue(number.Uint64()), false, nil
	case profile.BACnetSigned:
		number, err := integerValue(column, value)
		if err != nil {
			return bacnet.ApplicationValue{}, false, err
		}
		if !number.IsInt64() {
			return bacnet.ApplicationValue{}, false, fmt.Errorf("signed value is out of range")
		}
		return bacnet.SignedValue(number.Int64()), false, nil
	case profile.BACnetReal:
		number, err := floatingValue(column, value)
		if err != nil {
			return bacnet.ApplicationValue{}, false, err
		}
		converted := float32(number)
		if math.IsInf(number, 0) || math.IsNaN(number) || math.IsInf(float64(converted), 0) {
			return bacnet.ApplicationValue{}, false, fmt.Errorf("real value is not finite or is out of range")
		}
		return bacnet.RealValue(converted), false, nil
	case profile.BACnetDouble:
		number, err := floatingValue(column, value)
		if err != nil {
			return bacnet.ApplicationValue{}, false, err
		}
		if math.IsInf(number, 0) || math.IsNaN(number) {
			return bacnet.ApplicationValue{}, false, fmt.Errorf("double value must be finite")
		}
		return bacnet.DoubleValue(number), false, nil
	case profile.BACnetOctetString:
		typed, ok := value.GetKind().(*kublingv1.Value_VarbinaryValue)
		if !ok {
			return bacnet.ApplicationValue{}, false, fmt.Errorf("expected VARBINARY literal")
		}
		return bacnet.ApplicationValue{
			Kind:        bacnet.ValueOctetString,
			OctetString: append([]byte(nil), typed.VarbinaryValue...),
		}, false, nil
	case profile.BACnetCharacterString:
		typed, ok := value.GetKind().(*kublingv1.Value_StringValue)
		if !ok {
			return bacnet.ApplicationValue{}, false, fmt.Errorf("expected STRING literal")
		}
		return bacnet.ApplicationValue{
			Kind:      bacnet.ValueCharacterString,
			Character: bacnet.CharacterString{Encoding: 0, Value: typed.StringValue},
		}, false, nil
	case profile.BACnetEnumerated:
		number, err := enumeratedValue(column, value)
		if err != nil {
			return bacnet.ApplicationValue{}, false, err
		}
		return bacnet.EnumValue(number), false, nil
	default:
		return bacnet.ApplicationValue{}, false, fmt.Errorf(
			"BACnet type %q is not writable in this provider version",
			column.BACnetType,
		)
	}
}

func integerValue(column profile.Column, value *kublingv1.Value) (*big.Int, error) {
	var raw string
	switch column.Type.GetType() {
	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		typed, ok := value.GetKind().(*kublingv1.Value_IntegerValue)
		if !ok {
			return nil, fmt.Errorf("expected INTEGER literal")
		}
		raw = strconv.FormatInt(int64(typed.IntegerValue), 10)
	case kublingv1.ValueType_VALUE_TYPE_LONG:
		typed, ok := value.GetKind().(*kublingv1.Value_LongValue)
		if !ok {
			return nil, fmt.Errorf("expected LONG literal")
		}
		raw = strconv.FormatInt(typed.LongValue, 10)
	case kublingv1.ValueType_VALUE_TYPE_BIGINTEGER:
		typed, ok := value.GetKind().(*kublingv1.Value_BigintegerValue)
		if !ok {
			return nil, fmt.Errorf("expected BIGINTEGER literal")
		}
		raw = typed.BigintegerValue
	default:
		return nil, fmt.Errorf("column type %s is not an integer", column.Type.GetType())
	}
	result, ok := new(big.Int).SetString(strings.TrimSpace(raw), 10)
	if !ok {
		return nil, fmt.Errorf("invalid integer value %q", raw)
	}
	return result, nil
}

func floatingValue(column profile.Column, value *kublingv1.Value) (float64, error) {
	switch column.Type.GetType() {
	case kublingv1.ValueType_VALUE_TYPE_FLOAT:
		typed, ok := value.GetKind().(*kublingv1.Value_FloatValue)
		if !ok {
			return 0, fmt.Errorf("expected FLOAT literal")
		}
		return float64(typed.FloatValue), nil
	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		typed, ok := value.GetKind().(*kublingv1.Value_DoubleValue)
		if !ok {
			return 0, fmt.Errorf("expected DOUBLE literal")
		}
		return typed.DoubleValue, nil
	case kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL:
		typed, ok := value.GetKind().(*kublingv1.Value_BigdecimalValue)
		if !ok {
			return 0, fmt.Errorf("expected BIGDECIMAL literal")
		}
		result, err := strconv.ParseFloat(strings.TrimSpace(typed.BigdecimalValue), 64)
		if err != nil {
			return 0, fmt.Errorf("invalid BIGDECIMAL value %q", typed.BigdecimalValue)
		}
		return result, nil
	default:
		return 0, fmt.Errorf("column type %s is not floating-point", column.Type.GetType())
	}
}

func enumeratedValue(column profile.Column, value *kublingv1.Value) (uint32, error) {
	switch column.Type.GetType() {
	case kublingv1.ValueType_VALUE_TYPE_STRING:
		typed, ok := value.GetKind().(*kublingv1.Value_StringValue)
		if !ok {
			return 0, fmt.Errorf("expected STRING literal")
		}
		for number, label := range column.Enum {
			if strings.EqualFold(label, strings.TrimSpace(typed.StringValue)) {
				return number, nil
			}
		}
		return 0, fmt.Errorf("unknown enumeration label %q", typed.StringValue)
	case kublingv1.ValueType_VALUE_TYPE_INTEGER, kublingv1.ValueType_VALUE_TYPE_LONG:
		number, err := integerValue(column, value)
		if err != nil {
			return 0, err
		}
		if number.Sign() < 0 || !number.IsUint64() || number.Uint64() > math.MaxUint32 {
			return 0, fmt.Errorf("enumerated value is out of range")
		}
		return uint32(number.Uint64()), nil
	default:
		return 0, fmt.Errorf("column type %s cannot represent an enumeration", column.Type.GetType())
	}
}

func findTableMetadata(compiled *catalog.Catalog, name string) *providerv1.TableMetadata {
	for _, candidate := range compiled.Metadata().GetTables() {
		if strings.EqualFold(candidate.GetName(), name) {
			return candidate
		}
	}
	return nil
}

func findProfileTable(selected *profile.Profile, name string) (profile.Table, bool) {
	for _, candidate := range selected.Tables {
		if strings.EqualFold(candidate.Name, name) {
			return candidate, true
		}
	}
	return profile.Table{}, false
}

func findColumn(table profile.Table, name string) (profile.Column, bool) {
	for _, candidate := range table.Columns {
		if strings.EqualFold(candidate.Name, name) {
			return candidate, true
		}
	}
	return profile.Column{}, false
}

func findObjectType(selected *profile.Profile, name string) (profile.ObjectType, bool) {
	for _, candidate := range selected.ObjectTypes {
		if strings.EqualFold(candidate.Name, strings.TrimSpace(name)) {
			return candidate, true
		}
	}
	return profile.ObjectType{}, false
}

func tableContainsObjectType(table profile.Table, name string) bool {
	if table.AllObjectTypes {
		return true
	}
	for _, candidate := range table.ObjectTypes {
		if candidate.Name == name {
			return true
		}
	}
	return false
}

func findDevice(
	snapshot discovery.Snapshot,
	namespace string,
	scopeID string,
	instance uint32,
) (discovery.Device, bool) {
	for _, scope := range snapshot.Scopes {
		if scope.Namespace != namespace || scope.ScopeID != scopeID {
			continue
		}
		for _, device := range scope.Devices {
			if device.Instance == instance {
				return device, true
			}
		}
	}
	return discovery.Device{}, false
}

func writeError(ctx context.Context, err error) error {
	var unknown *bacnet.OutcomeUnknownError
	if errors.As(err, &unknown) {
		return status.Errorf(
			codes.Unknown,
			"BACnet WriteProperty outcome is unknown and was not retried: %v",
			err,
		)
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return status.FromContextError(contextErr).Err()
	}
	var remote *bacnet.ErrorResponse
	if errors.As(err, &remote) {
		if remote.Code == bacnet.ErrorCodeUnknownObject {
			return status.Errorf(codes.NotFound, "BACnet object does not exist: %v", err)
		}
		return status.Errorf(codes.FailedPrecondition, "BACnet device rejected WriteProperty: %v", err)
	}
	var reject *bacnet.RejectError
	var abort *bacnet.AbortError
	if errors.As(err, &reject) || errors.As(err, &abort) {
		return status.Errorf(codes.FailedPrecondition, "BACnet device rejected WriteProperty: %v", err)
	}
	return status.Errorf(codes.Unavailable, "BACnet WriteProperty failed: %v", err)
}

func affectedRows(count uint64) *providerv1.UpdateResponse {
	return &providerv1.UpdateResponse{AffectedRows: &count}
}

func invalid(format string, arguments ...any) error {
	return status.Errorf(codes.InvalidArgument, format, arguments...)
}
