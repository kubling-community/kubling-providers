package catalog

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"gopkg.in/yaml.v3"
)

func TestCompileStandardCatalog(t *testing.T) {
	compiled := compileTestCatalog(t)
	metadata := compiled.Metadata()
	wantTables := []string{
		ScopesTable,
		DevicesTable,
		ObjectsTable,
		DeviceObjectsTable,
		AnalogPointsTable,
		BinaryPointsTable,
		MultistatePointsTable,
	}
	gotTables := make([]string, 0, len(metadata.GetTables()))
	for _, table := range metadata.GetTables() {
		gotTables = append(gotTables, table.GetName())
		stableKey := columnByName(t, table, IdentifierColumn).GetStableKey()
		if stableKey == nil {
			t.Fatalf("table %s has no stable key", table.GetName())
		}
		switch table.GetName() {
		case ScopesTable:
			if !reflect.DeepEqual(stableKey.GetColumns(), []string{NamespaceColumn, ScopeIDColumn}) {
				t.Fatalf("SCOPES stable key = %v", stableKey.GetColumns())
			}
			if len(table.GetAccessPatterns()) != 0 {
				t.Fatalf("SCOPES access patterns = %v, want unrestricted", table.GetAccessPatterns())
			}
			continue
		case DevicesTable:
			if !reflect.DeepEqual(stableKey.GetColumns(), []string{
				NamespaceColumn,
				ScopeIDColumn,
				DeviceInstanceColumn,
			}) {
				t.Fatalf("DEVICES stable key = %v", stableKey.GetColumns())
			}
			if len(table.GetAccessPatterns()) != 0 {
				t.Fatalf("DEVICES access patterns = %v, want unrestricted", table.GetAccessPatterns())
			}
			continue
		}
		if !reflect.DeepEqual(stableKey.GetColumns(), objectIdentityColumns) {
			t.Fatalf("table %s stable key = %v", table.GetName(), stableKey.GetColumns())
		}
		assertRoutingPatterns(t, table)
	}
	if !reflect.DeepEqual(gotTables, wantTables) {
		t.Fatalf("tables = %v, want %v", gotTables, wantTables)
	}

	objects := tableByName(t, metadata, ObjectsTable)
	objectTypeCount := 0
	for _, column := range objects.GetColumns() {
		if column.GetName() == ObjectTypeColumn {
			objectTypeCount++
			if column.GetSourceName() != "object-type" ||
				column.GetProperties()["bacnet.property_id"] != "79" {
				t.Fatalf("OBJECTS object_type mapping = %v", column)
			}
		}
	}
	if objectTypeCount != 1 {
		t.Fatalf("OBJECTS object_type columns = %d, want 1", objectTypeCount)
	}

	devices := tableByName(t, metadata, DevicesTable)
	if !columnByName(t, devices, "last_seen_at").GetNullable() {
		t.Fatal("DEVICES last_seen_at must be nullable before the first successful observation")
	}
	deviceObjects := tableByName(t, metadata, DeviceObjectsTable)
	objectList := columnByName(t, deviceObjects, "object_list")
	if objectList.GetType() != kublingv1.ValueType_VALUE_TYPE_ARRAY ||
		objectList.GetTypeDescriptor().GetElementType().GetType() != kublingv1.ValueType_VALUE_TYPE_STRING {
		t.Fatalf("DEVICE_OBJECTS object_list descriptor = %v", objectList.GetTypeDescriptor())
	}
	assertForeignKey(t, devices, ScopesTable)
	assertForeignKey(t, objects, DevicesTable)
	assertForeignKey(t, deviceObjects, ObjectsTable)
	analog := tableByName(t, metadata, AnalogPointsTable)
	assertForeignKey(t, analog, ObjectsTable)
	presentValue := columnByName(t, analog, "present_value")
	if !analog.GetUpdatable() || !presentValue.GetUpdatable() ||
		presentValue.GetProperties()["bacnet.writable_object_types"] != "analog-output,analog-value" {
		t.Fatalf("ANALOG_POINTS mutation metadata = table:%v column:%v", analog, presentValue)
	}
	if objects.GetUpdatable() || columnByName(t, objects, "description").GetUpdatable() {
		t.Fatalf("OBJECTS unexpectedly advertises updates: %v", objects)
	}
}

func TestCompileUnionsInjectedTablesAndColumns(t *testing.T) {
	compiled := compileTestCatalog(t, `
apiVersion: kubling.io/bacnet-profile/v1
id: acme-vav
extends:
  - bacnet-standard
selectors:
  - vendorIdentifier: 4242
objectTypes:
  - id: 128
    name: acme-air-quality
tables:
  - name: ANALOG_POINTS
    objectTypes:
      - acme-air-quality
    columns:
      - name: fan_mode
        property:
          id: 512
          name: fan-mode
        bacnetType: enumerated
        kublingType: STRING
        required: true
        nullable: false
  - name: AIR_QUALITY_POINTS
    objectTypes:
      - acme-air-quality
    columns:
      - name: co2_ppm
        property:
          id: 513
          name: co2-ppm
        bacnetType: real
        kublingType: DOUBLE
        required: true
        nullable: false
`)
	metadata := compiled.Metadata()
	if len(metadata.GetTables()) != 8 {
		t.Fatalf("table count = %d, want 8", len(metadata.GetTables()))
	}
	analog := tableByName(t, metadata, AnalogPointsTable)
	if fanMode := columnByName(t, analog, "fan_mode"); !fanMode.GetNullable() {
		t.Fatalf("profile-specific ANALOG_POINTS fan_mode must be nullable in the union: %v", fanMode)
	}
	airQuality := tableByName(t, metadata, "AIR_QUALITY_POINTS")
	if co2 := columnByName(t, airQuality, "co2_ppm"); co2.GetNullable() {
		t.Fatalf("AIR_QUALITY_POINTS co2_ppm unexpectedly nullable: %v", co2)
	}
	assertRoutingPatterns(t, airQuality)
	assertForeignKey(t, airQuality, ObjectsTable)

	var document semanticDocument
	if err := yaml.Unmarshal(compiled.SemanticFragment().Document, &document); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}
	if semanticEntityByID(document.Spec.Entities, "AirQualityPoints") == nil {
		t.Fatal("semantic fragment does not contain injected AIR_QUALITY_POINTS entity")
	}
	if semanticRelationshipByID(document.Spec.Relationships, "ObjectHasAirQualityPoints") == nil {
		t.Fatal("semantic fragment does not relate OBJECTS to AIR_QUALITY_POINTS")
	}
}

func TestCompileRejectsProviderOwnedProfileColumns(t *testing.T) {
	profiles := loadTestProfiles(t, `
apiVersion: kubling.io/bacnet-profile/v1
id: invalid-routing-column
extends:
  - bacnet-standard
selectors:
  - vendorIdentifier: 4242
tables:
  - name: ANALOG_POINTS
    objectTypes:
      - analog-input
    columns:
      - name: namespace
        property:
          id: 512
          name: namespace
        bacnetType: character-string
        kublingType: STRING
`)
	if _, err := Compile(profiles); err == nil || !strings.Contains(err.Error(), "provider-owned") {
		t.Fatalf("Compile() error = %v, want provider-owned column rejection", err)
	}
}

func TestCompileRejectsProviderOwnedProfileTables(t *testing.T) {
	profiles := loadTestProfiles(t, `
apiVersion: kubling.io/bacnet-profile/v1
id: invalid-registry-table
extends:
  - bacnet-standard
selectors:
  - vendorIdentifier: 4242
tables:
  - name: DEVICES
    objectTypes:
      - device
    columns:
      - name: vendor_mode
        property:
          id: 512
          name: vendor-mode
        bacnetType: character-string
        kublingType: STRING
`)
	if _, err := Compile(profiles); err == nil || !strings.Contains(err.Error(), "provider-owned table") {
		t.Fatalf("Compile() error = %v, want provider-owned table rejection", err)
	}
}

func TestCompileMarksProfileDependentPropertyMappings(t *testing.T) {
	compiled := compileTestCatalog(t, `
apiVersion: kubling.io/bacnet-profile/v1
id: vendor-one
extends:
  - bacnet-standard
selectors:
  - vendorIdentifier: 1001
tables:
  - name: ANALOG_POINTS
    objectTypes:
      - analog-value
    columns:
      - name: vendor_mode
        property:
          id: 512
          name: vendor-one-mode
        bacnetType: enumerated
        kublingType: STRING
`, `
apiVersion: kubling.io/bacnet-profile/v1
id: vendor-two
extends:
  - bacnet-standard
selectors:
  - vendorIdentifier: 1002
tables:
  - name: ANALOG_POINTS
    objectTypes:
      - analog-value
    columns:
      - name: vendor_mode
        property:
          id: 513
          name: vendor-two-mode
        bacnetType: enumerated
        kublingType: STRING
`)
	column := columnByName(
		t,
		tableByName(t, compiled.Metadata(), AnalogPointsTable),
		"vendor_mode",
	)
	if column.GetNativeType() != "profile-dependent" ||
		column.GetProperties()["bacnet.mapping"] != "profile-dependent" ||
		column.GetSourceName() != "" {
		t.Fatalf("vendor_mode mapping = %v", column)
	}
}

func TestCatalogReturnsDetachedArtifacts(t *testing.T) {
	compiled := compileTestCatalog(t)
	firstMetadata := compiled.Metadata()
	firstMetadata.Tables[0].Name = "MUTATED"
	if compiled.Metadata().GetTables()[0].GetName() != ScopesTable {
		t.Fatal("Metadata() returned shared state")
	}

	firstSemantic := compiled.SemanticFragment()
	originalDocument := append([]byte(nil), firstSemantic.Document...)
	firstSemantic.Document[0] = '!'
	if !reflect.DeepEqual(compiled.SemanticFragment().Document, originalDocument) {
		t.Fatal("SemanticFragment() returned shared state")
	}
}

func TestSemanticFragmentMatchesCatalogIdentityAndRelationships(t *testing.T) {
	compiled := compileTestCatalog(t)
	fragment := compiled.SemanticFragment()
	if fragment.Version != semanticFragmentVersion || fragment.MediaType != "application/yaml" {
		t.Fatalf("semantic envelope = %#v", fragment)
	}
	var document semanticDocument
	if err := yaml.Unmarshal(fragment.Document, &document); err != nil {
		t.Fatalf("yaml.Unmarshal() error = %v", err)
	}
	if document.Format != "kubling-semantic" || document.SchemaVersion != 1 || document.Kind != "fragment" {
		t.Fatalf("semantic document header = %#v", document)
	}
	if len(document.Spec.Entities) != 7 {
		t.Fatalf("semantic entity count = %d, want 7", len(document.Spec.Entities))
	}
	device := semanticEntityByID(document.Spec.Entities, "Device")
	if device == nil || len(device.Identities) != 1 || len(device.Identities[0].Properties) != 3 {
		t.Fatalf("Device semantic identity = %#v", device)
	}
	if relationship := semanticRelationshipByID(document.Spec.Relationships, "ScopeHasDevices"); relationship == nil || len(relationship.Joins) != 2 {
		t.Fatalf("ScopeHasDevices = %#v", relationship)
	}
	if relationship := semanticRelationshipByID(document.Spec.Relationships, "DeviceHasObjects"); relationship == nil || len(relationship.Joins) != 3 {
		t.Fatalf("DeviceHasObjects = %#v", relationship)
	}
	if relationship := semanticRelationshipByID(document.Spec.Relationships, "ObjectHasAnalogPoint"); relationship == nil || len(relationship.Joins) != 5 {
		t.Fatalf("ObjectHasAnalogPoint = %#v", relationship)
	}
	if relationship := semanticRelationshipByID(document.Spec.Relationships, "ObjectHasDeviceObject"); relationship == nil || len(relationship.Joins) != 5 {
		t.Fatalf("ObjectHasDeviceObject = %#v", relationship)
	}
}

func compileTestCatalog(t *testing.T, documents ...string) *Catalog {
	t.Helper()
	profiles := loadTestProfiles(t, documents...)
	compiled, err := Compile(profiles)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return compiled
}

func loadTestProfiles(t *testing.T, documents ...string) *profile.Catalog {
	t.Helper()
	directory := t.TempDir()
	paths := make([]string, 0, len(documents))
	for index, document := range documents {
		path := filepath.Join(directory, "profile-"+string(rune('a'+index))+".yaml")
		if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
			t.Fatalf("os.WriteFile() error = %v", err)
		}
		paths = append(paths, path)
	}
	profiles, err := profile.Load(paths)
	if err != nil {
		t.Fatalf("profile.Load() error = %v", err)
	}
	return profiles
}

func tableByName(
	t *testing.T,
	metadata *providerv1.SchemaMetadata,
	name string,
) *providerv1.TableMetadata {
	t.Helper()
	for _, table := range metadata.GetTables() {
		if table.GetName() == name {
			return table
		}
	}
	t.Fatalf("metadata table %q not found", name)
	return nil
}

func columnByName(
	t *testing.T,
	table *providerv1.TableMetadata,
	name string,
) *providerv1.ColumnMetadata {
	t.Helper()
	for _, column := range table.GetColumns() {
		if column.GetName() == name {
			return column
		}
	}
	t.Fatalf("table %s column %q not found", table.GetName(), name)
	return nil
}

func assertRoutingPatterns(t *testing.T, table *providerv1.TableMetadata) {
	t.Helper()
	want := [][]string{
		{NamespaceColumn, ScopeIDColumn, DeviceInstanceColumn},
	}
	if len(table.GetAccessPatterns()) != len(want) {
		t.Fatalf("table %s access patterns = %v", table.GetName(), table.GetAccessPatterns())
	}
	for index, columns := range want {
		if !reflect.DeepEqual(table.GetAccessPatterns()[index].GetColumns(), columns) {
			t.Fatalf(
				"table %s access pattern %d = %v, want %v",
				table.GetName(),
				index,
				table.GetAccessPatterns()[index].GetColumns(),
				columns,
			)
		}
	}
}

func assertForeignKey(t *testing.T, table *providerv1.TableMetadata, referencedTable string) {
	t.Helper()
	for _, key := range table.GetKeys() {
		if key.GetKind() == providerv1.KeyKind_KEY_KIND_FOREIGN &&
			key.GetReferencedTable() == referencedTable {
			return
		}
	}
	t.Fatalf("table %s has no foreign key to %s", table.GetName(), referencedTable)
}

func semanticEntityByID(entities []semanticEntity, id string) *semanticEntity {
	for index := range entities {
		if entities[index].ID == id {
			return &entities[index]
		}
	}
	return nil
}

func semanticRelationshipByID(
	relationships []semanticRelationship,
	id string,
) *semanticRelationship {
	for index := range relationships {
		if relationships[index].ID == id {
			return &relationships[index]
		}
	}
	return nil
}
