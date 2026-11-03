package query

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/catalog"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/discovery"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	bacnet "github.com/otfabric/go-bacnet"
	"github.com/otfabric/go-bacnet/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const maximumObjectsPerDevice = 4096

// DeviceSource is the stable registry plus the narrow physical read surface
// needed by one-device queries.
type DeviceSource interface {
	Snapshot() discovery.Snapshot
	ReadObjectList(context.Context, discovery.Device, int) ([]bacnet.ObjectIdentifier, error)
	ReadProperties(
		context.Context,
		discovery.Device,
		[]service.ReadAccessSpecification,
	) ([]service.ReadAccessResult, error)
}

// Execute plans and materializes one bounded BACnet query. Multi-device fan-out
// intentionally remains outside this executor.
func Execute(
	ctx context.Context,
	compiled *catalog.Catalog,
	devices DeviceSource,
	request *providerv1.QueryRequest,
) (providersdk.ResultStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if devices == nil {
		return nil, status.Error(codes.FailedPrecondition, "BACnet device registry is unavailable")
	}
	planned, err := buildPlan(compiled, request)
	if err != nil {
		var featureError *requiredFeatureError
		if errors.As(err, &featureError) {
			return nil, status.Errorf(codes.FailedPrecondition, "plan BACnet query: %v", err)
		}
		return nil, status.Errorf(codes.InvalidArgument, "plan BACnet query: %v", err)
	}
	if planned.impossible {
		return newResultStream(planned.fields(), nil, queryBatchSize(request)), nil
	}
	snapshot := devices.Snapshot()
	var tuples []*providerv1.Tuple
	switch planned.table {
	case catalog.ScopesTable:
		tuples, err = scopeTuples(ctx, planned, snapshot)
	case catalog.DevicesTable:
		tuples, err = registryDeviceTuples(ctx, planned, snapshot)
	default:
		tuples, err = deviceTuples(ctx, planned, compiled, devices, snapshot)
	}
	if err != nil {
		return nil, err
	}
	return newResultStream(planned.fields(), tuples, queryBatchSize(request)), nil
}

func registryDeviceTuples(
	ctx context.Context,
	planned plan,
	snapshot discovery.Snapshot,
) ([]*providerv1.Tuple, error) {
	observedAt := snapshot.CapturedAt
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	var result []*providerv1.Tuple
	for _, scope := range snapshot.Scopes {
		for _, device := range scope.Devices {
			if err := ctx.Err(); err != nil {
				return nil, status.FromContextError(err).Err()
			}
			values := map[string]*kublingv1.Value{
				catalog.NamespaceColumn:      stringValue(device.Namespace),
				catalog.ScopeIDColumn:        stringValue(device.ScopeID),
				catalog.DeviceInstanceColumn: integerValue(int32(device.Instance)),
				"attachment_id":              stringValue(device.AttachmentID),
				"profile_id":                 nullableStringValue(device.ProfileID),
				"route":                      stringValue(device.Route.Canonical()),
				"vendor_identifier":          nullableUnsignedIntegerValue(device.Identity.VendorIdentifier),
				"vendor_name":                nullableStringValue(device.Identity.VendorName),
				"model_name":                 nullableStringValue(device.Identity.ModelName),
				"firmware_revision":          nullableStringValue(device.Identity.FirmwareRevision),
				"application_software_version": nullableStringValue(
					device.Identity.ApplicationSoftwareVersion,
				),
				"protocol_version":       nullableUnsignedIntegerValue(device.Identity.ProtocolVersion),
				"protocol_revision":      nullableUnsignedIntegerValue(device.Identity.ProtocolRevision),
				"last_seen_at":           nullableTimestampValue(device.LastSeen),
				"static_route":           booleanValue(device.Static),
				catalog.ObservedAtColumn: timestampValue(observedAt),
			}
			if !matchesConstraints(planned, values) {
				continue
			}
			tuple, err := projectTuple(planned, values)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "project BACnet registry device: %v", err)
			}
			result = append(result, tuple)
		}
	}
	return result, nil
}

func scopeTuples(
	ctx context.Context,
	planned plan,
	snapshot discovery.Snapshot,
) ([]*providerv1.Tuple, error) {
	observedAt := snapshot.CapturedAt
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}
	result := make([]*providerv1.Tuple, 0, len(snapshot.Scopes))
	for _, scope := range snapshot.Scopes {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		values := map[string]*kublingv1.Value{
			catalog.NamespaceColumn:  stringValue(scope.Namespace),
			catalog.ScopeIDColumn:    stringValue(scope.ScopeID),
			"attachment_id":          stringValue(scope.AttachmentID),
			"last_attempt_at":        nullableTimestampValue(scope.LastAttempt),
			"last_success_at":        nullableTimestampValue(scope.LastSuccess),
			"device_count":           integerValue(int32(len(scope.Devices))),
			"duplicate_device_count": integerValue(int32(len(scope.Duplicates))),
			catalog.ObservedAtColumn: timestampValue(observedAt),
		}
		if !matchesConstraints(planned, values) {
			continue
		}
		tuple, err := projectTuple(planned, values)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "project BACnet scope: %v", err)
		}
		result = append(result, tuple)
	}
	return result, nil
}

func deviceTuples(
	ctx context.Context,
	planned plan,
	compiled *catalog.Catalog,
	source DeviceSource,
	snapshot discovery.Snapshot,
) ([]*providerv1.Tuple, error) {
	namespace, namespaceOK := constraintString(planned, catalog.NamespaceColumn)
	scopeID, scopeOK := constraintString(planned, catalog.ScopeIDColumn)
	instance, instanceOK := constraintInteger(planned, catalog.DeviceInstanceColumn)
	if !namespaceOK || !scopeOK || !instanceOK {
		return nil, status.Error(
			codes.FailedPrecondition,
			"BACnet query requires equality criteria for namespace, scope_id and device_instance",
		)
	}
	device, exists := findDevice(snapshot, namespace, scopeID, instance)
	if !exists {
		return nil, nil
	}
	selected, exists := compiled.Profiles().Profile(device.ProfileID)
	if !exists || selected == nil || selected.Abstract {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"registered BACnet device %s/%s/%d has no usable profile",
			namespace,
			scopeID,
			instance,
		)
	}
	declared, exists := profileTable(selected, planned.table)
	if !exists {
		return nil, nil
	}

	objects := []bacnet.ObjectIdentifier{{
		Type:     bacnet.ObjectTypeDevice,
		Instance: uint32(device.Instance),
	}}
	var deviceObjects []bacnet.ObjectIdentifier
	if planned.table != catalog.DeviceObjectsTable || needsObjectList(planned, declared) {
		var err error
		deviceObjects, err = source.ReadObjectList(ctx, device, maximumObjectsPerDevice)
		if err != nil {
			return nil, sourceError(ctx, "read BACnet object-list", err)
		}
		if len(deviceObjects) > maximumObjectsPerDevice {
			return nil, status.Errorf(
				codes.ResourceExhausted,
				"BACnet device %s/%s/%d exceeds the object limit of %d",
				namespace,
				scopeID,
				instance,
				maximumObjectsPerDevice,
			)
		}
		slices.SortFunc(deviceObjects, compareObjectIdentifiers)
		for index := 1; index < len(deviceObjects); index++ {
			if deviceObjects[index] == deviceObjects[index-1] {
				return nil, status.Errorf(
					codes.FailedPrecondition,
					"BACnet object-list contains duplicate object %s",
					formatObjectIdentifier(selected, deviceObjects[index]),
				)
			}
		}
	}
	if planned.table != catalog.DeviceObjectsTable {
		objects = filterObjects(deviceObjects, declared, planned, selected)
	}

	propertyColumns := neededPropertyColumns(planned, declared)
	specifications := make([]service.ReadAccessSpecification, 0, len(objects))
	for _, object := range objects {
		properties := make([]bacnet.PropertyReference, 0, len(propertyColumns))
		seen := make(map[uint32]struct{}, len(propertyColumns))
		for _, column := range propertyColumns {
			if specialObjectListColumn(column, object) {
				continue
			}
			if _, duplicate := seen[column.Property.ID]; duplicate {
				continue
			}
			seen[column.Property.ID] = struct{}{}
			properties = append(properties, bacnet.PropertyReference{
				Identifier: bacnet.PropertyIdentifier(column.Property.ID),
			})
		}
		sort.Slice(properties, func(left int, right int) bool {
			return properties[left].Identifier < properties[right].Identifier
		})
		if len(properties) != 0 {
			specifications = append(specifications, service.ReadAccessSpecification{
				Object:     object,
				Properties: properties,
			})
		}
	}

	propertyResults := make(map[bacnet.ObjectIdentifier]map[bacnet.PropertyIdentifier]service.PropertyResult)
	if len(specifications) != 0 {
		results, err := source.ReadProperties(ctx, device, specifications)
		if err != nil {
			return nil, sourceError(ctx, "read BACnet properties", err)
		}
		for _, result := range results {
			byProperty := make(map[bacnet.PropertyIdentifier]service.PropertyResult, len(result.Properties))
			for _, property := range result.Properties {
				byProperty[property.Property.Identifier] = property
			}
			propertyResults[result.Object] = byProperty
		}
	}

	observedAt := time.Now().UTC()
	tuples := make([]*providerv1.Tuple, 0, len(objects))
	for _, object := range objects {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		values := objectIdentityValues(device, object, selected, observedAt)
		byProperty := propertyResults[object]
		for _, column := range declared.Columns {
			name := strings.ToLower(column.Name)
			if _, needed := planned.needed[name]; !needed || name == catalog.ObjectTypeColumn {
				continue
			}
			if specialObjectListColumn(column, object) {
				value, err := objectListValue(column, deviceObjects, selected)
				if err != nil {
					return nil, status.Errorf(codes.FailedPrecondition, "convert %s.%s: %v", planned.table, column.Name, err)
				}
				values[name] = value
				continue
			}
			property, present := byProperty[bacnet.PropertyIdentifier(column.Property.ID)]
			if !present || property.Err != nil {
				if column.Nullable && !column.Required {
					values[name] = nullValue()
					continue
				}
				reason := error(nil)
				if present {
					reason = property.Err
				}
				if reason == nil {
					reason = fmt.Errorf("property was not returned")
				}
				return nil, status.Errorf(
					codes.FailedPrecondition,
					"required BACnet property %s (%d) for %s is unavailable: %v",
					column.Property.Name,
					column.Property.ID,
					formatObjectIdentifier(selected, object),
					reason,
				)
			}
			if column.Property.ID == uint32(bacnet.PropertyObjectIdentifier) &&
				(property.Value.Kind != bacnet.ValueObjectID || property.Value.ObjectID != object) {
				return nil, status.Errorf(
					codes.FailedPrecondition,
					"BACnet object %s returned a mismatched object-identifier",
					formatObjectIdentifier(selected, object),
				)
			}
			converted, err := convertBACnetValue(column, property.Value, selected)
			if err != nil {
				return nil, status.Errorf(
					codes.FailedPrecondition,
					"convert BACnet property %s (%d) for %s: %v",
					column.Property.Name,
					column.Property.ID,
					formatObjectIdentifier(selected, object),
					err,
				)
			}
			values[name] = converted
		}
		fillProfileUnionNulls(planned, values)
		if !matchesConstraints(planned, values) {
			continue
		}
		tuple, err := projectTuple(planned, values)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "project BACnet object: %v", err)
		}
		tuples = append(tuples, tuple)
	}
	return tuples, nil
}

func findDevice(
	snapshot discovery.Snapshot,
	namespace string,
	scopeID string,
	instance int32,
) (discovery.Device, bool) {
	if instance < 0 {
		return discovery.Device{}, false
	}
	for _, scope := range snapshot.Scopes {
		if scope.Namespace != namespace || scope.ScopeID != scopeID {
			continue
		}
		for _, device := range scope.Devices {
			if device.Instance == uint32(instance) {
				return device, true
			}
		}
	}
	return discovery.Device{}, false
}

func profileTable(selected *profile.Profile, name string) (profile.Table, bool) {
	for _, table := range selected.Tables {
		if table.Name == name {
			return table, true
		}
	}
	return profile.Table{}, false
}

func filterObjects(
	objects []bacnet.ObjectIdentifier,
	table profile.Table,
	planned plan,
	selected *profile.Profile,
) []bacnet.ObjectIdentifier {
	allowed := make(map[uint32]struct{}, len(table.ObjectTypes))
	for _, objectType := range table.ObjectTypes {
		allowed[objectType.ID] = struct{}{}
	}
	result := make([]bacnet.ObjectIdentifier, 0, len(objects))
	for _, object := range objects {
		if !table.AllObjectTypes {
			if _, exists := allowed[uint32(object.Type)]; !exists {
				continue
			}
		}
		values := map[string]*kublingv1.Value{
			catalog.ObjectTypeColumn:     stringValue(objectTypeName(selected, object.Type)),
			catalog.ObjectInstanceColumn: integerValue(int32(object.Instance)),
			"object_identifier":          stringValue(formatObjectIdentifier(selected, object)),
		}
		matches := true
		for _, name := range []string{catalog.ObjectTypeColumn, catalog.ObjectInstanceColumn, "object_identifier"} {
			constraint := planned.constraints[name]
			if constraint != nil && !proto.Equal(constraint, values[name]) {
				matches = false
				break
			}
		}
		if matches {
			result = append(result, object)
		}
	}
	return result
}

func compareObjectIdentifiers(left bacnet.ObjectIdentifier, right bacnet.ObjectIdentifier) int {
	if left.Type < right.Type {
		return -1
	}
	if left.Type > right.Type {
		return 1
	}
	if left.Instance < right.Instance {
		return -1
	}
	if left.Instance > right.Instance {
		return 1
	}
	return 0
}

func neededPropertyColumns(planned plan, table profile.Table) []profile.Column {
	result := make([]profile.Column, 0, len(table.Columns))
	for _, column := range table.Columns {
		if _, needed := planned.needed[strings.ToLower(column.Name)]; !needed ||
			column.Name == catalog.ObjectTypeColumn {
			continue
		}
		result = append(result, column)
	}
	return result
}

func needsObjectList(planned plan, table profile.Table) bool {
	for _, column := range table.Columns {
		if _, needed := planned.needed[strings.ToLower(column.Name)]; needed &&
			column.Property.ID == uint32(bacnet.PropertyObjectList) {
			return true
		}
	}
	return false
}

func specialObjectListColumn(column profile.Column, object bacnet.ObjectIdentifier) bool {
	return object.Type == bacnet.ObjectTypeDevice &&
		column.Property.ID == uint32(bacnet.PropertyObjectList) &&
		column.Type.GetType() == kublingv1.ValueType_VALUE_TYPE_ARRAY
}

func objectListValue(
	column profile.Column,
	objects []bacnet.ObjectIdentifier,
	selected *profile.Profile,
) (*kublingv1.Value, error) {
	value := bacnet.ApplicationValue{Kind: bacnet.ValueConstructed}
	for _, object := range objects {
		value.Elements = append(value.Elements, bacnet.Element{Value: bacnet.ObjectIDValue(object)})
	}
	return convertBACnetValue(column, value, selected)
}

func objectIdentityValues(
	device discovery.Device,
	object bacnet.ObjectIdentifier,
	selected *profile.Profile,
	observedAt time.Time,
) map[string]*kublingv1.Value {
	return map[string]*kublingv1.Value{
		catalog.NamespaceColumn:      stringValue(device.Namespace),
		catalog.ScopeIDColumn:        stringValue(device.ScopeID),
		catalog.DeviceInstanceColumn: integerValue(int32(device.Instance)),
		catalog.ObjectTypeColumn:     stringValue(objectTypeName(selected, object.Type)),
		catalog.ObjectInstanceColumn: integerValue(int32(object.Instance)),
		catalog.ObservedAtColumn:     timestampValue(observedAt),
	}
}

func fillProfileUnionNulls(planned plan, values map[string]*kublingv1.Value) {
	for name := range planned.needed {
		if values[name] != nil {
			continue
		}
		values[name] = nullValue()
	}
}

func matchesConstraints(planned plan, values map[string]*kublingv1.Value) bool {
	for column, expected := range planned.constraints {
		actual := values[column]
		if actual == nil || !proto.Equal(actual, expected) {
			return false
		}
	}
	return true
}

func projectTuple(planned plan, values map[string]*kublingv1.Value) (*providerv1.Tuple, error) {
	result := &providerv1.Tuple{Values: make([]*kublingv1.Value, 0, len(planned.projections))}
	for _, projected := range planned.projections {
		value := values[projected.column]
		if value == nil {
			return nil, fmt.Errorf("projected value %q is missing", projected.column)
		}
		result.Values = append(result.Values, proto.Clone(value).(*kublingv1.Value))
	}
	return result, nil
}

func constraintString(planned plan, name string) (string, bool) {
	value := planned.constraints[name]
	if value == nil {
		return "", false
	}
	typed, ok := value.GetKind().(*kublingv1.Value_StringValue)
	if !ok {
		return "", false
	}
	return typed.StringValue, true
}

func constraintInteger(planned plan, name string) (int32, bool) {
	value := planned.constraints[name]
	if value == nil {
		return 0, false
	}
	typed, ok := value.GetKind().(*kublingv1.Value_IntegerValue)
	if !ok {
		return 0, false
	}
	return typed.IntegerValue, true
}

func sourceError(ctx context.Context, operation string, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return status.FromContextError(contextErr).Err()
	}
	if errors.Is(err, bacnet.ErrLimitExceeded) {
		return status.Errorf(codes.ResourceExhausted, "%s: %v", operation, err)
	}
	return status.Errorf(codes.Unavailable, "%s: %v", operation, err)
}

func queryBatchSize(request *providerv1.QueryRequest) int {
	if request == nil || request.BatchSize == nil || request.GetBatchSize() == 0 {
		return 100
	}
	return int(min(request.GetBatchSize(), 1000))
}
