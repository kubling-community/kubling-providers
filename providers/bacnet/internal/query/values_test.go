package query

import (
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	bacnet "github.com/otfabric/go-bacnet"
)

func TestConvertBACnetScalarValues(t *testing.T) {
	selected := &profile.Profile{ObjectTypes: []profile.ObjectType{{ID: 2, Name: "analog-value"}}}
	tests := []struct {
		name       string
		column     profile.Column
		value      bacnet.ApplicationValue
		wantString string
		wantInt    int32
		wantBytes  []byte
	}{
		{
			name:       "mapped enum",
			column:     valueColumn(profile.BACnetEnumerated, kublingv1.ValueType_VALUE_TYPE_STRING),
			value:      bacnet.EnumValue(1),
			wantString: "active",
		},
		{
			name:       "object identifier",
			column:     valueColumn(profile.BACnetObjectIdentifier, kublingv1.ValueType_VALUE_TYPE_STRING),
			value:      bacnet.ObjectIDValue(bacnet.ObjectIdentifier{Type: 2, Instance: 17}),
			wantString: "analog-value:17",
		},
		{
			name:      "bit string",
			column:    valueColumn(profile.BACnetBitString, kublingv1.ValueType_VALUE_TYPE_VARBINARY),
			value:     bacnet.ApplicationValue{Kind: bacnet.ValueBitString, BitString: bacnet.BitString{UnusedBits: 4, Bytes: []byte{0xa0}}},
			wantBytes: []byte{0xa0},
		},
		{
			name:       "date",
			column:     valueColumn(profile.BACnetDate, kublingv1.ValueType_VALUE_TYPE_DATE),
			value:      bacnet.ApplicationValue{Kind: bacnet.ValueDate, Date: bacnet.Date{Year: 126, Month: 11, Day: 2}},
			wantString: "2026-11-02",
		},
		{
			name:       "time",
			column:     valueColumn(profile.BACnetTime, kublingv1.ValueType_VALUE_TYPE_TIME),
			value:      bacnet.ApplicationValue{Kind: bacnet.ValueTime, Time: bacnet.Time{Hour: 14, Minute: 3, Second: 5, Hundredths: 25}},
			wantString: "14:03:05.25",
		},
		{
			name:    "unsigned",
			column:  valueColumn(profile.BACnetUnsigned, kublingv1.ValueType_VALUE_TYPE_INTEGER),
			value:   bacnet.UnsignedValue(42),
			wantInt: 42,
		},
	}
	tests[0].column.Enum = map[uint32]string{1: "active"}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted, err := convertBACnetValue(test.column, test.value, selected)
			if err != nil {
				t.Fatalf("convertBACnetValue() error = %v", err)
			}
			switch test.column.Type.GetType() {
			case kublingv1.ValueType_VALUE_TYPE_STRING:
				if converted.GetStringValue() != test.wantString {
					t.Fatalf("value = %q, want %q", converted.GetStringValue(), test.wantString)
				}
			case kublingv1.ValueType_VALUE_TYPE_VARBINARY:
				if string(converted.GetVarbinaryValue()) != string(test.wantBytes) {
					t.Fatalf("value = %x, want %x", converted.GetVarbinaryValue(), test.wantBytes)
				}
			case kublingv1.ValueType_VALUE_TYPE_DATE:
				if converted.GetDateValue() != test.wantString {
					t.Fatalf("value = %q, want %q", converted.GetDateValue(), test.wantString)
				}
			case kublingv1.ValueType_VALUE_TYPE_TIME:
				if converted.GetTimeValue() != test.wantString {
					t.Fatalf("value = %q, want %q", converted.GetTimeValue(), test.wantString)
				}
			case kublingv1.ValueType_VALUE_TYPE_INTEGER:
				if converted.GetIntegerValue() != test.wantInt {
					t.Fatalf("value = %d, want %d", converted.GetIntegerValue(), test.wantInt)
				}
			}
		})
	}
}

func TestConvertBACnetArrayPreservesElementType(t *testing.T) {
	column := valueColumn(profile.BACnetCharacterString, kublingv1.ValueType_VALUE_TYPE_STRING)
	column.Type = &kublingv1.TypeDescriptor{
		Type:        kublingv1.ValueType_VALUE_TYPE_ARRAY,
		ElementType: column.Type,
	}
	value := bacnet.ApplicationValue{
		Kind: bacnet.ValueConstructed,
		Elements: []bacnet.Element{
			{Value: textApplicationValue("off")},
			{Value: textApplicationValue("on")},
		},
	}
	converted, err := convertBACnetValue(column, value, nil)
	if err != nil {
		t.Fatalf("convertBACnetValue() error = %v", err)
	}
	array := converted.GetArrayValue()
	if array.GetElementType().GetType() != kublingv1.ValueType_VALUE_TYPE_STRING ||
		len(array.GetElements()) != 2 || array.GetElements()[1].GetStringValue() != "on" {
		t.Fatalf("array = %v", array)
	}
}

func valueColumn(native profile.BACnetType, logical kublingv1.ValueType) profile.Column {
	return profile.Column{
		Name:       "value",
		Property:   profile.Property{ID: 1, Name: "value"},
		BACnetType: native,
		Type:       &kublingv1.TypeDescriptor{Type: logical},
		Nullable:   true,
	}
}
