package profile

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
)

func TestLoadCompilesBuiltInStandardProfile(t *testing.T) {
	catalog, err := Load(nil)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	profiles := catalog.Profiles()
	if len(profiles) != 1 || profiles[0].ID != StandardProfileID {
		t.Fatalf("Profiles() = %#v", profiles)
	}
	standard, exists := catalog.Profile(StandardProfileID)
	if !exists {
		t.Fatalf("Profile(%q) missing", StandardProfileID)
	}
	for _, name := range []string{
		"ANALOG_POINTS",
		"BINARY_POINTS",
		"DEVICE_OBJECTS",
		"MULTISTATE_POINTS",
		"OBJECTS",
	} {
		if findTable(standard, name) == nil {
			t.Fatalf("standard profile missing table %q", name)
		}
	}
	objectList := findColumn(findTable(standard, "DEVICE_OBJECTS"), "object_list")
	if objectList == nil ||
		objectList.Type.GetType() != kublingv1.ValueType_VALUE_TYPE_ARRAY ||
		objectList.Type.GetElementType().GetType() != kublingv1.ValueType_VALUE_TYPE_STRING {
		t.Fatalf("DEVICE_OBJECTS.object_list type = %v", objectList)
	}
	presentValue := findColumn(findTable(standard, "ANALOG_POINTS"), "present_value")
	if presentValue == nil || presentValue.Write == nil || !presentValue.Write.Commandable ||
		!presentValue.WritableFor("analog-output") ||
		!presentValue.WritableFor("analog-value") ||
		presentValue.WritableFor("analog-input") {
		t.Fatalf("ANALOG_POINTS.present_value write policy = %#v", presentValue)
	}
	selected, err := catalog.Select(DeviceIdentity{})
	if err != nil || selected.ID != StandardProfileID {
		t.Fatalf("Select() = %v, %v", selected, err)
	}
}

func TestLoadCompilesInheritanceOverridesAndProvenance(t *testing.T) {
	path := writeProfile(t, "acme.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: acme-vav
extends:
  - bacnet-standard
selectors:
  - vendorIdentifier: 4242
    modelName: VAV-1000
objectTypes:
  - id: 128
    name: acme-vav
tables:
  - name: ANALOG_POINTS
    objectTypes:
      - acme-vav
    columns:
      - name: present_value
        property:
          id: 513
          name: precise-present-value
        bacnetType: double
        kublingType: DOUBLE
        override: true
      - name: fan_mode
        property:
          id: 512
          name: fan-mode
        bacnetType: enumerated
        kublingType: STRING
        enum:
          0: off
          1: auto
          2: on
`)
	catalog, err := Load([]string{path})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	selected, err := catalog.Select(DeviceIdentity{
		VendorIdentifier: 4242,
		ModelName:        "VAV-1000",
	})
	if err != nil || selected.ID != "acme-vav" {
		t.Fatalf("Select() = %v, %v", selected, err)
	}
	table := findTable(selected, "ANALOG_POINTS")
	if table == nil || findObjectType(table, "acme-vav") == nil {
		t.Fatalf("ANALOG_POINTS = %#v", table)
	}
	presentValue := findColumn(table, "present_value")
	if presentValue == nil ||
		presentValue.Type.GetType() != kublingv1.ValueType_VALUE_TYPE_DOUBLE ||
		presentValue.Property.ID != 513 ||
		presentValue.SourceProfile != "acme-vav" ||
		!reflect.DeepEqual(
			presentValue.SourceProfiles,
			[]string{"acme-vav", StandardProfileID},
		) || presentValue.Write != nil {
		t.Fatalf("present_value = %#v", presentValue)
	}
	fanMode := findColumn(table, "fan_mode")
	if fanMode == nil || fanMode.Enum[1] != "auto" || !fanMode.Nullable {
		t.Fatalf("fan_mode = %#v", fanMode)
	}
}

func TestLoadRejectsInvalidWritePolicies(t *testing.T) {
	for name, column := range map[string]string{
		"unknown object type": `
      - name: vendor_value
        property: {id: 512, name: vendor-value}
        bacnetType: real
        kublingType: DOUBLE
        write:
          objectTypes: [missing-object]`,
		"object type outside table": `
      - name: vendor_value
        property: {id: 512, name: vendor-value}
        bacnetType: real
        kublingType: DOUBLE
        write:
          objectTypes: [binary-output]`,
		"writable array": `
      - name: vendor_values
        property: {id: 512, name: vendor-values}
        bacnetType: real
        kublingType: DOUBLE
        array: true
        write:
          objectTypes: [analog-output]`,
		"unsupported writable type": `
      - name: vendor_bits
        property: {id: 512, name: vendor-bits}
        bacnetType: bit-string
        kublingType: VARBINARY
        write:
          objectTypes: [analog-output]`,
	} {
		t.Run(name, func(t *testing.T) {
			path := writeProfile(t, "invalid-write.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: invalid-write
extends: [bacnet-standard]
selectors:
  - vendorIdentifier: 9
tables:
  - name: ANALOG_POINTS
    columns:`+column+"\n")
			if _, err := Load([]string{path}); err == nil {
				t.Fatal("Load() error = nil")
			}
		})
	}
}

func TestLoadRejectsIncompatibleColumnWithoutExplicitOverride(t *testing.T) {
	path := writeProfile(t, "conflict.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: conflicting-profile
extends: [bacnet-standard]
selectors:
  - vendorIdentifier: 7
tables:
  - name: ANALOG_POINTS
    columns:
      - name: present_value
        property:
          id: 85
          name: present-value
        bacnetType: real
        kublingType: FLOAT
`)
	_, err := Load([]string{path})
	if err == nil || !strings.Contains(err.Error(), "set override: true") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRejectsCatalogTypeConflictsAcrossDisjointProfiles(t *testing.T) {
	left := writeProfile(t, "left.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: left-profile
extends: [bacnet-standard]
selectors:
  - vendorIdentifier: 10
tables:
  - name: DEVICE_OBJECTS
    columns:
      - name: vendor_mode
        property:
          id: 512
          name: vendor-mode
        bacnetType: character-string
        kublingType: STRING
`)
	right := writeProfile(t, "right.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: right-profile
extends: [bacnet-standard]
selectors:
  - vendorIdentifier: 11
tables:
  - name: DEVICE_OBJECTS
    columns:
      - name: vendor_mode
        property:
          id: 513
          name: vendor-mode
        bacnetType: unsigned
        kublingType: INTEGER
`)
	_, err := Load([]string{left, right})
	if err == nil || !strings.Contains(err.Error(), "incompatible logical types") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRejectsAmbiguousSelectors(t *testing.T) {
	left := writeProfile(t, "left.yaml", selectableProfile("left", 77))
	right := writeProfile(t, "right.yaml", selectableProfile("right", 77))
	_, err := Load([]string{left, right})
	if err == nil || !strings.Contains(err.Error(), "ambiguous selectors") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadTreatsAllDeviceSelectorsAsTheSamePrecedence(t *testing.T) {
	left := writeProfile(t, "left.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: device-left
extends: [bacnet-standard]
selectors:
  - namespace: test
    scopeId: local
    deviceInstance: 12001
`)
	right := writeProfile(t, "right.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: device-right
extends: [bacnet-standard]
selectors:
  - vendorIdentifier: 77
    namespace: test
    scopeId: local
    deviceInstance: 12001
`)
	_, err := Load([]string{left, right})
	if err == nil || !strings.Contains(err.Error(), "ambiguous selectors") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRejectsUnscopedDeviceOverrides(t *testing.T) {
	path := writeProfile(t, "unscoped-device.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: unscoped-device
extends: [bacnet-standard]
selectors:
  - deviceInstance: 12001
`)
	_, err := Load([]string{path})
	if err == nil || !strings.Contains(err.Error(), "requires a valid namespace") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestSelectUsesTheConfiguredPrecedenceHierarchy(t *testing.T) {
	vendor := writeProfile(t, "vendor.yaml", selectableProfile("vendor-profile", 77))
	model := writeProfile(t, "model.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: model-profile
extends: [vendor-profile]
selectors:
  - vendorIdentifier: 77
    modelName: VAV-1000
`)
	firmware := writeProfile(t, "firmware.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: firmware-profile
extends: [model-profile]
selectors:
  - vendorIdentifier: 77
    modelName: VAV-1000
    firmwareRevision: 2.0.1
`)
	device := writeProfile(t, "device.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: device-profile
extends: [firmware-profile]
selectors:
  - namespace: test
    scopeId: local
    deviceInstance: 12001
`)
	catalog, err := Load([]string{device, vendor, firmware, model})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	tests := []struct {
		name     string
		identity DeviceIdentity
		want     string
	}{
		{name: "standard", identity: DeviceIdentity{VendorIdentifier: 12}, want: StandardProfileID},
		{name: "vendor", identity: DeviceIdentity{VendorIdentifier: 77}, want: "vendor-profile"},
		{
			name:     "model",
			identity: DeviceIdentity{VendorIdentifier: 77, ModelName: "VAV-1000"},
			want:     "model-profile",
		},
		{
			name: "firmware",
			identity: DeviceIdentity{
				VendorIdentifier: 77,
				ModelName:        "VAV-1000",
				FirmwareRevision: "2.0.1",
			},
			want: "firmware-profile",
		},
		{
			name: "device",
			identity: DeviceIdentity{
				VendorIdentifier: 77,
				ModelName:        "VAV-1000",
				FirmwareRevision: "2.0.1",
				DeviceInstance:   12001,
				Namespace:        "test",
				ScopeID:          "local",
			},
			want: "device-profile",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			selected, err := catalog.Select(test.identity)
			if err != nil || selected.ID != test.want {
				t.Fatalf("Select() = %v, %v; want %q", selected, err, test.want)
			}
		})
	}
}

func TestLoadRejectsChildSelectorThatDoesNotIncreasePrecedence(t *testing.T) {
	parent := writeProfile(t, "parent.yaml", selectableProfile("vendor-profile", 77))
	child := writeProfile(t, "child.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: child-profile
extends: [vendor-profile]
selectors:
  - vendorIdentifier: 77
`)
	_, err := Load([]string{parent, child})
	if err == nil || !strings.Contains(err.Error(), "more specific selector") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRejectsInheritanceCycles(t *testing.T) {
	left := writeProfile(t, "left.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: cycle-left
abstract: true
extends: [bacnet-standard, cycle-right]
`)
	right := writeProfile(t, "right.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: cycle-right
abstract: true
extends: [cycle-left]
`)
	_, err := Load([]string{left, right})
	if err == nil || !strings.Contains(err.Error(), "inheritance cycle") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadIsIndependentOfFileOrder(t *testing.T) {
	left := writeProfile(t, "left.yaml", selectableProfile("left", 1))
	right := writeProfile(t, "right.yaml", selectableProfile("right", 2))
	first, err := Load([]string{left, right})
	if err != nil {
		t.Fatalf("first Load() error = %v", err)
	}
	second, err := Load([]string{right, left})
	if err != nil {
		t.Fatalf("second Load() error = %v", err)
	}
	if !reflect.DeepEqual(first.Profiles(), second.Profiles()) {
		t.Fatal("compiled profiles depend on file order")
	}
}

func TestLoadRejectsUnknownFieldsAndMultipleDocuments(t *testing.T) {
	tests := map[string]string{
		"unknown": `
apiVersion: kubling.io/bacnet-profile/v1
id: unknown-field
extends: [bacnet-standard]
selectors:
  - vendorIdentifier: 1
unknown: true
`,
		"multiple": selectableProfile("first", 1) + "\n---\n" + selectableProfile("second", 2),
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Load([]string{writeProfile(t, name+".yaml", contents)})
			if err == nil {
				t.Fatal("Load() error = nil")
			}
		})
	}
}

func TestLoadRejectsInvalidTypeMappings(t *testing.T) {
	path := writeProfile(t, "invalid-type.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: invalid-type
extends: [bacnet-standard]
selectors:
  - vendorIdentifier: 9
tables:
  - name: DEVICE_OBJECTS
    columns:
      - name: invalid_value
        property:
          id: 600
          name: invalid-value
        bacnetType: boolean
        kublingType: STRING
`)
	_, err := Load([]string{path})
	if err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("Load() error = %v", err)
	}
}

func TestLoadRejectsSelectorsOutsideThePrecedenceHierarchy(t *testing.T) {
	path := writeProfile(t, "invalid-selector.yaml", `
apiVersion: kubling.io/bacnet-profile/v1
id: invalid-selector
extends: [bacnet-standard]
selectors:
  - modelName: VAV-1000
`)
	_, err := Load([]string{path})
	if err == nil || !strings.Contains(err.Error(), "modelName requires vendorIdentifier") {
		t.Fatalf("Load() error = %v", err)
	}
}

func selectableProfile(id string, vendor uint32) string {
	return fmt.Sprintf(`apiVersion: kubling.io/bacnet-profile/v1
id: %s
extends: [bacnet-standard]
selectors:
  - vendorIdentifier: %d
`, id, vendor)
}

func writeProfile(t *testing.T, name string, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
	return path
}

func findTable(profile *Profile, name string) *Table {
	if profile == nil {
		return nil
	}
	for index := range profile.Tables {
		if profile.Tables[index].Name == name {
			return &profile.Tables[index]
		}
	}
	return nil
}

func findColumn(table *Table, name string) *Column {
	if table == nil {
		return nil
	}
	for index := range table.Columns {
		if table.Columns[index].Name == name {
			return &table.Columns[index]
		}
	}
	return nil
}

func findObjectType(table *Table, name string) *ObjectType {
	if table == nil {
		return nil
	}
	for index := range table.ObjectTypes {
		if table.ObjectTypes[index].Name == name {
			return &table.ObjectTypes[index]
		}
	}
	return nil
}
