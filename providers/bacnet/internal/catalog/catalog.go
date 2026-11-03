// Package catalog compiles BACnet profiles into the stable relational catalog
// exposed through the Kubling provider API.
package catalog

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/protobuf/proto"
)

const (
	ScopesTable           = "SCOPES"
	DevicesTable          = "DEVICES"
	DeviceObjectsTable    = "DEVICE_OBJECTS"
	ObjectsTable          = "OBJECTS"
	AnalogPointsTable     = "ANALOG_POINTS"
	BinaryPointsTable     = "BINARY_POINTS"
	MultistatePointsTable = "MULTISTATE_POINTS"

	IdentifierColumn     = "identifier"
	NamespaceColumn      = "namespace"
	ScopeIDColumn        = "scope_id"
	DeviceInstanceColumn = "device_instance"
	ObjectTypeColumn     = "object_type"
	ObjectInstanceColumn = "object_instance"
	ObservedAtColumn     = "observed_at"
)

var objectIdentityColumns = []string{
	NamespaceColumn,
	ScopeIDColumn,
	DeviceInstanceColumn,
	ObjectTypeColumn,
	ObjectInstanceColumn,
}

var providerOwnedColumns = map[string]struct{}{
	IdentifierColumn:     {},
	NamespaceColumn:      {},
	ScopeIDColumn:        {},
	DeviceInstanceColumn: {},
	ObjectTypeColumn:     {},
	ObjectInstanceColumn: {},
	ObservedAtColumn:     {},
}

// Catalog owns the immutable profile model and the derived public artifacts.
type Catalog struct {
	profiles *profile.Catalog
	metadata *providerv1.SchemaMetadata
	semantic *providersdk.SemanticFragment
}

// Compile derives metadata and semantics without opening network listeners or
// BACnet attachments.
func Compile(profiles *profile.Catalog) (*Catalog, error) {
	if profiles == nil {
		return nil, fmt.Errorf("BACnet profile catalog is required")
	}

	metadata, err := buildMetadata(profiles)
	if err != nil {
		return nil, err
	}
	semantic, err := buildSemanticFragment(metadata)
	if err != nil {
		return nil, err
	}
	if err := providersdk.ValidateSemanticFragment(semantic); err != nil {
		return nil, fmt.Errorf("validate BACnet semantic fragment: %w", err)
	}

	return &Catalog{
		profiles: profiles,
		metadata: metadata,
		semantic: semantic,
	}, nil
}

// Profiles returns the compiled device profiles used to build this catalog.
func (c *Catalog) Profiles() *profile.Catalog {
	if c == nil {
		return nil
	}
	return c.profiles
}

// Metadata returns a detached catalog copy.
func (c *Catalog) Metadata() *providerv1.SchemaMetadata {
	if c == nil || c.metadata == nil {
		return nil
	}
	return proto.Clone(c.metadata).(*providerv1.SchemaMetadata)
}

// SemanticFragment returns a detached semantic artifact copy.
func (c *Catalog) SemanticFragment() *providersdk.SemanticFragment {
	if c == nil {
		return nil
	}
	return providersdk.CloneSemanticFragment(c.semantic)
}

type tableAccumulator struct {
	name           string
	allObjectTypes bool
	objectTypes    map[string]profile.ObjectType
	profileIDs     map[string]struct{}
	columns        map[string]*columnAccumulator
}

type columnAccumulator struct {
	column              profile.Column
	profileIDs          map[string]struct{}
	writableProfileIDs  map[string]struct{}
	writableObjectTypes map[string]struct{}
	occurrences         int
	nullable            bool
	mappings            map[string]struct{}
}

func buildMetadata(profiles *profile.Catalog) (*providerv1.SchemaMetadata, error) {
	tables := make(map[string]*tableAccumulator)
	concreteProfiles := 0
	for _, candidate := range profiles.Profiles() {
		if candidate == nil || candidate.Abstract {
			continue
		}
		concreteProfiles++
		for _, declared := range candidate.Tables {
			if declared.Name == ScopesTable || declared.Name == DevicesTable {
				return nil, fmt.Errorf("profile %q uses provider-owned table %q", candidate.ID, declared.Name)
			}
			accumulator := tables[declared.Name]
			if accumulator == nil {
				accumulator = &tableAccumulator{
					name:        declared.Name,
					objectTypes: make(map[string]profile.ObjectType),
					profileIDs:  make(map[string]struct{}),
					columns:     make(map[string]*columnAccumulator),
				}
				tables[declared.Name] = accumulator
			}
			accumulator.allObjectTypes = accumulator.allObjectTypes || declared.AllObjectTypes
			accumulator.profileIDs[candidate.ID] = struct{}{}
			for _, objectType := range declared.ObjectTypes {
				if previous, exists := accumulator.objectTypes[objectType.Name]; exists && previous.ID != objectType.ID {
					return nil, fmt.Errorf(
						"table %q maps object type %q to ids %d and %d across profiles",
						declared.Name,
						objectType.Name,
						previous.ID,
						objectType.ID,
					)
				}
				accumulator.objectTypes[objectType.Name] = objectType
			}
			for _, column := range declared.Columns {
				if err := validateProfileColumn(declared.Name, column); err != nil {
					return nil, fmt.Errorf("profile %q: %w", candidate.ID, err)
				}
				accumulated := accumulator.columns[column.Name]
				if accumulated == nil {
					accumulated = &columnAccumulator{
						column:              column,
						profileIDs:          make(map[string]struct{}),
						writableProfileIDs:  make(map[string]struct{}),
						writableObjectTypes: make(map[string]struct{}),
						mappings:            make(map[string]struct{}),
					}
					accumulator.columns[column.Name] = accumulated
				}
				if !proto.Equal(accumulated.column.Type, column.Type) {
					return nil, fmt.Errorf(
						"table %q column %q has incompatible logical types across profiles",
						declared.Name,
						column.Name,
					)
				}
				accumulated.profileIDs[candidate.ID] = struct{}{}
				accumulated.occurrences++
				accumulated.nullable = accumulated.nullable || column.Nullable
				accumulated.mappings[propertyMappingKey(column)] = struct{}{}
				if column.Write != nil {
					accumulated.writableProfileIDs[candidate.ID] = struct{}{}
					for _, objectType := range column.Write.ObjectTypes {
						accumulated.writableObjectTypes[objectType] = struct{}{}
					}
				}
			}
		}
	}
	if concreteProfiles == 0 {
		return nil, fmt.Errorf("BACnet profile catalog has no concrete profiles")
	}

	metadata := &providerv1.SchemaMetadata{
		Properties: map[string]string{
			"bacnet.profile_count": strconv.Itoa(concreteProfiles),
			"bacnet.schema_model":  "profile-union",
		},
		Tables: []*providerv1.TableMetadata{scopeMetadata(), deviceMetadata()},
	}
	ordered := make([]*tableAccumulator, 0, len(tables))
	for _, table := range tables {
		ordered = append(ordered, table)
	}
	slices.SortFunc(ordered, compareTables)
	for _, accumulated := range ordered {
		table, err := profileTableMetadata(accumulated)
		if err != nil {
			return nil, err
		}
		metadata.Tables = append(metadata.Tables, table)
	}
	return metadata, nil
}

func validateProfileColumn(tableName string, column profile.Column) error {
	if column.Name == ObjectTypeColumn && tableName == ObjectsTable {
		if column.Type.GetType() != kublingv1.ValueType_VALUE_TYPE_STRING {
			return fmt.Errorf("table %q column %q must be STRING", tableName, column.Name)
		}
		return nil
	}
	if _, reserved := providerOwnedColumns[column.Name]; reserved {
		return fmt.Errorf("table %q column %q is provider-owned", tableName, column.Name)
	}
	return nil
}

func propertyMappingKey(column profile.Column) string {
	return fmt.Sprintf(
		"%d\x00%s\x00%s",
		column.Property.ID,
		column.Property.Name,
		column.BACnetType,
	)
}

func compareTables(left *tableAccumulator, right *tableAccumulator) int {
	leftRank := tableRank(left.name)
	rightRank := tableRank(right.name)
	if leftRank != rightRank {
		return leftRank - rightRank
	}
	return strings.Compare(left.name, right.name)
}

func tableRank(name string) int {
	switch name {
	case ObjectsTable:
		return 0
	case DeviceObjectsTable:
		return 1
	case AnalogPointsTable:
		return 2
	case BinaryPointsTable:
		return 3
	case MultistatePointsTable:
		return 4
	default:
		return 100
	}
}

func scopeMetadata() *providerv1.TableMetadata {
	updatable := false
	table := &providerv1.TableMetadata{
		Name:       ScopesTable,
		SourceName: ScopesTable,
		Kind:       providerv1.TableKind_TABLE_KIND_TABLE,
		Updatable:  &updatable,
		Annotation: "Configured logical BACnet discovery scopes and their registry state",
		Columns: []*providerv1.ColumnMetadata{
			columnMetadata(NamespaceColumn, stringType(), false, true, "Logical BACnet namespace", "provider routing key"),
			columnMetadata(ScopeIDColumn, stringType(), false, true, "Logical BACnet scope identifier", "provider routing key"),
			columnMetadata("attachment_id", stringType(), false, false, "Physical BACnet/IP attachment used by the scope", "provider attachment"),
			columnMetadata("last_attempt_at", timestampType(), true, false, "Most recent discovery attempt", "provider timestamp"),
			columnMetadata("last_success_at", timestampType(), true, false, "Most recent successful discovery", "provider timestamp"),
			columnMetadata("device_count", integerType(), false, false, "Unambiguous devices in the current registry snapshot", "provider count"),
			columnMetadata("duplicate_device_count", integerType(), false, false, "Device instances rejected because multiple routes answered", "provider count"),
			columnMetadata(ObservedAtColumn, timestampType(), false, false, "Registry snapshot timestamp", "provider timestamp"),
		},
		Keys: []*providerv1.KeyMetadata{
			uniqueKey("UK_SCOPES_IDENTITY", NamespaceColumn, ScopeIDColumn),
		},
	}
	providersdk.MustAddStablePrimaryKey(table, IdentifierColumn, NamespaceColumn, ScopeIDColumn)
	return table
}

func deviceMetadata() *providerv1.TableMetadata {
	updatable := false
	table := &providerv1.TableMetadata{
		Name:       DevicesTable,
		SourceName: DevicesTable,
		Kind:       providerv1.TableKind_TABLE_KIND_TABLE,
		Updatable:  &updatable,
		Annotation: "Devices in the current BACnet discovery registry",
		Columns: []*providerv1.ColumnMetadata{
			columnMetadata(NamespaceColumn, stringType(), false, true, "Logical BACnet namespace", "provider routing key"),
			columnMetadata(ScopeIDColumn, stringType(), false, true, "Logical BACnet scope identifier", "provider routing key"),
			columnMetadata(DeviceInstanceColumn, integerType(), false, true, "BACnet Device Instance", "BACnet device instance"),
			columnMetadata("attachment_id", stringType(), false, false, "Physical BACnet/IP attachment used by the device", "provider attachment"),
			columnMetadata("profile_id", stringType(), true, false, "Compiled BACnet profile selected for the device", "provider profile"),
			columnMetadata("route", stringType(), false, false, "Canonical route used to reach the device", "BACnet route"),
			columnMetadata("vendor_identifier", integerType(), true, false, "Vendor identifier observed during discovery", "BACnet unsigned"),
			columnMetadata("vendor_name", stringType(), true, false, "Vendor name observed during discovery", "BACnet character-string"),
			columnMetadata("model_name", stringType(), true, false, "Model name observed during discovery", "BACnet character-string"),
			columnMetadata("firmware_revision", stringType(), true, false, "Firmware revision observed during discovery", "BACnet character-string"),
			columnMetadata("application_software_version", stringType(), true, false, "Application software version observed during discovery", "BACnet character-string"),
			columnMetadata("protocol_version", integerType(), true, false, "BACnet protocol version observed during discovery", "BACnet unsigned"),
			columnMetadata("protocol_revision", integerType(), true, false, "BACnet protocol revision observed during discovery", "BACnet unsigned"),
			columnMetadata("last_seen_at", timestampType(), true, false, "Most recent confirmed device observation", "provider timestamp"),
			columnMetadata("static_route", booleanType(), false, false, "Whether the route is configured statically", "provider flag"),
			columnMetadata(ObservedAtColumn, timestampType(), false, false, "Registry snapshot timestamp", "provider timestamp"),
		},
		Keys: []*providerv1.KeyMetadata{
			uniqueKey("UK_DEVICES_DEVICE_IDENTITY", NamespaceColumn, ScopeIDColumn, DeviceInstanceColumn),
			foreignKey(
				"FK_DEVICES_SCOPE",
				[]string{NamespaceColumn, ScopeIDColumn},
				ScopesTable,
				[]string{NamespaceColumn, ScopeIDColumn},
			),
		},
	}
	providersdk.MustAddStablePrimaryKey(
		table,
		IdentifierColumn,
		NamespaceColumn,
		ScopeIDColumn,
		DeviceInstanceColumn,
	)
	return table
}

func profileTableMetadata(accumulated *tableAccumulator) (*providerv1.TableMetadata, error) {
	updatable := false
	for _, column := range accumulated.columns {
		if len(column.writableProfileIDs) != 0 {
			updatable = true
			break
		}
	}
	table := &providerv1.TableMetadata{
		Name:           accumulated.name,
		SourceName:     accumulated.name,
		Kind:           providerv1.TableKind_TABLE_KIND_TABLE,
		Updatable:      &updatable,
		Annotation:     tableAnnotation(accumulated.name),
		Properties:     tableProperties(accumulated),
		AccessPatterns: routingAccessPatterns(accumulated.name),
		Columns: []*providerv1.ColumnMetadata{
			columnMetadata(NamespaceColumn, stringType(), false, true, "Logical BACnet namespace", "provider routing key"),
			columnMetadata(ScopeIDColumn, stringType(), false, true, "Logical BACnet scope identifier", "provider routing key"),
			columnMetadata(DeviceInstanceColumn, integerType(), false, true, "BACnet Device Instance", "BACnet device instance"),
			columnMetadata(ObjectTypeColumn, stringType(), false, true, "BACnet object type", "BACnet object type"),
			columnMetadata(ObjectInstanceColumn, integerType(), false, true, "BACnet object instance", "BACnet object instance"),
		},
	}

	columnNames := make([]string, 0, len(accumulated.columns))
	for name := range accumulated.columns {
		if name != ObjectTypeColumn {
			columnNames = append(columnNames, name)
		}
	}
	sort.Strings(columnNames)
	for _, name := range columnNames {
		table.Columns = append(
			table.Columns,
			profileColumnMetadata(accumulated.columns[name], len(accumulated.profileIDs)),
		)
	}
	if objectType := accumulated.columns[ObjectTypeColumn]; objectType != nil {
		decorateProfileMapping(table.Columns[3], objectType)
	}
	table.Columns = append(
		table.Columns,
		columnMetadata(ObservedAtColumn, timestampType(), false, false, "Observation timestamp for this row", "provider timestamp"),
	)

	identity := append([]string(nil), objectIdentityColumns...)
	table.Keys = append(table.Keys, uniqueKey("UK_"+table.Name+"_OBJECT_IDENTITY", identity...))
	switch table.Name {
	case ObjectsTable:
		table.Keys = append(table.Keys, foreignKey(
			"FK_OBJECTS_DEVICE",
			[]string{NamespaceColumn, ScopeIDColumn, DeviceInstanceColumn},
			DevicesTable,
			[]string{NamespaceColumn, ScopeIDColumn, DeviceInstanceColumn},
		))
	default:
		table.Keys = append(table.Keys, foreignKey(
			"FK_"+table.Name+"_OBJECT",
			identity,
			ObjectsTable,
			identity,
		))
	}
	if err := providersdk.AddStablePrimaryKey(table, IdentifierColumn, identity...); err != nil {
		return nil, fmt.Errorf("build BACnet table %q: %w", table.Name, err)
	}
	return table, nil
}

func tableAnnotation(name string) string {
	switch name {
	case DevicesTable:
		return "Devices in the current BACnet discovery registry"
	case DeviceObjectsTable:
		return "BACnet Device Object properties read from one registered device"
	case ObjectsTable:
		return "BACnet objects exposed by selected device profiles"
	case AnalogPointsTable:
		return "BACnet analog input, output and value objects"
	case BinaryPointsTable:
		return "BACnet binary input, output and value objects"
	case MultistatePointsTable:
		return "BACnet multi-state input, output and value objects"
	default:
		return "BACnet objects exposed by configured device profiles"
	}
}

func tableProperties(accumulated *tableAccumulator) map[string]string {
	profiles := sortedSet(accumulated.profileIDs)
	objectTypes := make([]string, 0, len(accumulated.objectTypes))
	for _, objectType := range accumulated.objectTypes {
		objectTypes = append(objectTypes, fmt.Sprintf("%s:%d", objectType.Name, objectType.ID))
	}
	sort.Strings(objectTypes)
	return map[string]string{
		"bacnet.all_object_types": strconv.FormatBool(accumulated.allObjectTypes),
		"bacnet.object_types":     strings.Join(objectTypes, ","),
		"bacnet.profile_ids":      strings.Join(profiles, ","),
	}
}

func profileColumnMetadata(
	accumulated *columnAccumulator,
	tableProfileCount int,
) *providerv1.ColumnMetadata {
	nullable := accumulated.nullable || accumulated.occurrences < tableProfileCount
	updatable := len(accumulated.writableProfileIDs) != 0
	searchability := providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE
	if accumulated.column.Name == "object_identifier" {
		searchability = providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY
	}
	column := &providerv1.ColumnMetadata{
		Name:           accumulated.column.Name,
		Type:           accumulated.column.Type.GetType(),
		TypeDescriptor: cloneTypeDescriptor(accumulated.column.Type),
		Nullable:       &nullable,
		Updatable:      &updatable,
		Searchability:  searchability,
		Properties: map[string]string{
			"bacnet.profile_ids": strings.Join(sortedSet(accumulated.profileIDs), ","),
		},
	}
	if updatable {
		column.Properties["bacnet.writable_profile_ids"] = strings.Join(
			sortedSet(accumulated.writableProfileIDs),
			",",
		)
		column.Properties["bacnet.writable_object_types"] = strings.Join(
			sortedSet(accumulated.writableObjectTypes),
			",",
		)
	}
	decorateProfileMapping(column, accumulated)
	return column
}

func decorateProfileMapping(
	column *providerv1.ColumnMetadata,
	accumulated *columnAccumulator,
) {
	if column.Properties == nil {
		column.Properties = make(map[string]string)
	}
	column.Properties["bacnet.profile_ids"] = strings.Join(
		sortedSet(accumulated.profileIDs),
		",",
	)
	if len(accumulated.mappings) == 1 {
		column.SourceName = accumulated.column.Property.Name
		column.NativeType = string(accumulated.column.BACnetType)
		column.Annotation = fmt.Sprintf(
			"BACnet property %s (%d)",
			accumulated.column.Property.Name,
			accumulated.column.Property.ID,
		)
		column.Properties["bacnet.property_id"] = strconv.FormatUint(
			uint64(accumulated.column.Property.ID),
			10,
		)
		column.Properties["bacnet.property_name"] = accumulated.column.Property.Name
		return
	}
	column.NativeType = "profile-dependent"
	column.Annotation = "BACnet property selected by the matched device profile"
	column.Properties["bacnet.mapping"] = "profile-dependent"
}

func routingAccessPatterns(tableName string) []*providerv1.AccessPatternMetadata {
	return []*providerv1.AccessPatternMetadata{
		{
			Name: "AP_" + tableName + "_DEVICE",
			Columns: []string{
				NamespaceColumn,
				ScopeIDColumn,
				DeviceInstanceColumn,
			},
		},
	}
}

func columnMetadata(
	name string,
	descriptor *kublingv1.TypeDescriptor,
	nullable bool,
	searchable bool,
	annotation string,
	nativeType string,
) *providerv1.ColumnMetadata {
	updatable := false
	searchability := providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE
	if searchable {
		searchability = providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY
	}
	return &providerv1.ColumnMetadata{
		Name:           name,
		Type:           descriptor.GetType(),
		TypeDescriptor: cloneTypeDescriptor(descriptor),
		NativeType:     nativeType,
		Nullable:       &nullable,
		Updatable:      &updatable,
		Searchability:  searchability,
		Annotation:     annotation,
	}
}

func uniqueKey(name string, columns ...string) *providerv1.KeyMetadata {
	return &providerv1.KeyMetadata{
		Name:    name,
		Kind:    providerv1.KeyKind_KEY_KIND_UNIQUE,
		Columns: append([]string(nil), columns...),
	}
}

func foreignKey(
	name string,
	columns []string,
	referencedTable string,
	referencedColumns []string,
) *providerv1.KeyMetadata {
	return &providerv1.KeyMetadata{
		Name:              name,
		Kind:              providerv1.KeyKind_KEY_KIND_FOREIGN,
		Columns:           append([]string(nil), columns...),
		ReferencedTable:   referencedTable,
		ReferencedColumns: append([]string(nil), referencedColumns...),
	}
}

func sortedSet(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func stringType() *kublingv1.TypeDescriptor {
	return &kublingv1.TypeDescriptor{Type: kublingv1.ValueType_VALUE_TYPE_STRING}
}

func integerType() *kublingv1.TypeDescriptor {
	return &kublingv1.TypeDescriptor{Type: kublingv1.ValueType_VALUE_TYPE_INTEGER}
}

func booleanType() *kublingv1.TypeDescriptor {
	return &kublingv1.TypeDescriptor{Type: kublingv1.ValueType_VALUE_TYPE_BOOLEAN}
}

func timestampType() *kublingv1.TypeDescriptor {
	return &kublingv1.TypeDescriptor{Type: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP}
}

func cloneTypeDescriptor(input *kublingv1.TypeDescriptor) *kublingv1.TypeDescriptor {
	if input == nil {
		return nil
	}
	return proto.Clone(input).(*kublingv1.TypeDescriptor)
}
