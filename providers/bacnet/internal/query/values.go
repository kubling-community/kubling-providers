package query

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/profile"
	bacnet "github.com/otfabric/go-bacnet"
	"google.golang.org/protobuf/proto"
)

func convertBACnetValue(
	column profile.Column,
	value bacnet.ApplicationValue,
	selected *profile.Profile,
) (*kublingv1.Value, error) {
	if value.Kind == bacnet.ValueNull {
		if !column.Nullable {
			return nil, fmt.Errorf("non-nullable property %q returned NULL", column.Property.Name)
		}
		return nullValue(), nil
	}
	if column.Type.GetType() != kublingv1.ValueType_VALUE_TYPE_ARRAY {
		return convertScalar(column, column.Type, value, selected)
	}
	elementType := column.Type.GetElementType()
	if elementType == nil {
		return nil, fmt.Errorf("array column %q has no element type", column.Name)
	}
	values := []bacnet.ApplicationValue{value}
	if value.Kind == bacnet.ValueConstructed || value.Kind == bacnet.ValueContext {
		values = values[:0]
		for _, element := range value.Elements {
			if element.Opening || element.Closing {
				continue
			}
			values = append(values, element.Value)
		}
	}
	elements := make([]*kublingv1.Value, 0, len(values))
	for index, item := range values {
		converted, err := convertScalar(column, elementType, item, selected)
		if err != nil {
			return nil, fmt.Errorf("array element %d: %w", index, err)
		}
		elements = append(elements, converted)
	}
	return &kublingv1.Value{Kind: &kublingv1.Value_ArrayValue{ArrayValue: &kublingv1.ArrayValue{
		ElementType: proto.Clone(elementType).(*kublingv1.TypeDescriptor),
		Elements:    elements,
	}}}, nil
}

func convertScalar(
	column profile.Column,
	descriptor *kublingv1.TypeDescriptor,
	value bacnet.ApplicationValue,
	selected *profile.Profile,
) (*kublingv1.Value, error) {
	if value.Kind == bacnet.ValueNull {
		return nullValue(), nil
	}
	switch column.BACnetType {
	case profile.BACnetBoolean:
		if value.Kind != bacnet.ValueBoolean {
			return nil, unexpectedKind(column, value)
		}
		return booleanValue(value.Boolean), nil
	case profile.BACnetUnsigned:
		if value.Kind != bacnet.ValueUnsigned {
			return nil, unexpectedKind(column, value)
		}
		return unsignedValue(descriptor.GetType(), value.Unsigned)
	case profile.BACnetSigned:
		if value.Kind != bacnet.ValueSigned {
			return nil, unexpectedKind(column, value)
		}
		return signedValue(descriptor.GetType(), value.Signed)
	case profile.BACnetReal:
		if value.Kind != bacnet.ValueReal {
			return nil, unexpectedKind(column, value)
		}
		return realValue(descriptor.GetType(), value.Real)
	case profile.BACnetDouble:
		if value.Kind != bacnet.ValueDouble {
			return nil, unexpectedKind(column, value)
		}
		return doubleValue(descriptor.GetType(), value.Double)
	case profile.BACnetOctetString:
		if value.Kind != bacnet.ValueOctetString {
			return nil, unexpectedKind(column, value)
		}
		return varbinaryValue(value.OctetString), nil
	case profile.BACnetCharacterString:
		if value.Kind != bacnet.ValueCharacterString {
			return nil, unexpectedKind(column, value)
		}
		text, err := value.Character.Text()
		if err != nil {
			return nil, err
		}
		return stringValue(text), nil
	case profile.BACnetBitString:
		if value.Kind != bacnet.ValueBitString {
			return nil, unexpectedKind(column, value)
		}
		if descriptor.GetType() == kublingv1.ValueType_VALUE_TYPE_STRING {
			return stringValue(formatBitString(value.BitString)), nil
		}
		return varbinaryValue(value.BitString.Bytes), nil
	case profile.BACnetEnumerated:
		if value.Kind != bacnet.ValueEnumerated {
			return nil, unexpectedKind(column, value)
		}
		return enumeratedValue(column, descriptor.GetType(), value.Enumerated)
	case profile.BACnetDate:
		if value.Kind != bacnet.ValueDate {
			return nil, unexpectedKind(column, value)
		}
		formatted, err := formatDate(value.Date, descriptor.GetType() == kublingv1.ValueType_VALUE_TYPE_STRING)
		if err != nil {
			return nil, err
		}
		if descriptor.GetType() == kublingv1.ValueType_VALUE_TYPE_STRING {
			return stringValue(formatted), nil
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_DateValue{DateValue: formatted}}, nil
	case profile.BACnetTime:
		if value.Kind != bacnet.ValueTime {
			return nil, unexpectedKind(column, value)
		}
		formatted, err := formatTime(value.Time, descriptor.GetType() == kublingv1.ValueType_VALUE_TYPE_STRING)
		if err != nil {
			return nil, err
		}
		if descriptor.GetType() == kublingv1.ValueType_VALUE_TYPE_STRING {
			return stringValue(formatted), nil
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_TimeValue{TimeValue: formatted}}, nil
	case profile.BACnetObjectIdentifier:
		if value.Kind != bacnet.ValueObjectID {
			return nil, unexpectedKind(column, value)
		}
		return stringValue(formatObjectIdentifier(selected, value.ObjectID)), nil
	default:
		return nil, fmt.Errorf("unsupported BACnet type %q", column.BACnetType)
	}
}

func unexpectedKind(column profile.Column, value bacnet.ApplicationValue) error {
	return fmt.Errorf(
		"property %q expected BACnet %s, received value kind %d",
		column.Property.Name,
		column.BACnetType,
		value.Kind,
	)
}

func unsignedValue(kind kublingv1.ValueType, value uint64) (*kublingv1.Value, error) {
	switch kind {
	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		if value > math.MaxInt32 {
			return nil, fmt.Errorf("unsigned value %d exceeds INTEGER", value)
		}
		return integerValue(int32(value)), nil
	case kublingv1.ValueType_VALUE_TYPE_LONG:
		if value > math.MaxInt64 {
			return nil, fmt.Errorf("unsigned value %d exceeds LONG", value)
		}
		return longValue(int64(value)), nil
	case kublingv1.ValueType_VALUE_TYPE_BIGINTEGER:
		return &kublingv1.Value{Kind: &kublingv1.Value_BigintegerValue{
			BigintegerValue: strconv.FormatUint(value, 10),
		}}, nil
	default:
		return nil, fmt.Errorf("cannot map unsigned value to %s", kind)
	}
}

func signedValue(kind kublingv1.ValueType, value int64) (*kublingv1.Value, error) {
	switch kind {
	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		if value < math.MinInt32 || value > math.MaxInt32 {
			return nil, fmt.Errorf("signed value %d exceeds INTEGER", value)
		}
		return integerValue(int32(value)), nil
	case kublingv1.ValueType_VALUE_TYPE_LONG:
		return longValue(value), nil
	case kublingv1.ValueType_VALUE_TYPE_BIGINTEGER:
		return &kublingv1.Value{Kind: &kublingv1.Value_BigintegerValue{
			BigintegerValue: strconv.FormatInt(value, 10),
		}}, nil
	default:
		return nil, fmt.Errorf("cannot map signed value to %s", kind)
	}
}

func realValue(kind kublingv1.ValueType, value float32) (*kublingv1.Value, error) {
	if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
		return nil, fmt.Errorf("BACnet real value must be finite")
	}
	switch kind {
	case kublingv1.ValueType_VALUE_TYPE_FLOAT:
		return &kublingv1.Value{Kind: &kublingv1.Value_FloatValue{FloatValue: value}}, nil
	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		return &kublingv1.Value{Kind: &kublingv1.Value_DoubleValue{DoubleValue: float64(value)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL:
		return &kublingv1.Value{Kind: &kublingv1.Value_BigdecimalValue{
			BigdecimalValue: strconv.FormatFloat(float64(value), 'g', -1, 32),
		}}, nil
	default:
		return nil, fmt.Errorf("cannot map real value to %s", kind)
	}
}

func doubleValue(kind kublingv1.ValueType, value float64) (*kublingv1.Value, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, fmt.Errorf("BACnet double value must be finite")
	}
	switch kind {
	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		return &kublingv1.Value{Kind: &kublingv1.Value_DoubleValue{DoubleValue: value}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL:
		return &kublingv1.Value{Kind: &kublingv1.Value_BigdecimalValue{
			BigdecimalValue: strconv.FormatFloat(value, 'g', -1, 64),
		}}, nil
	default:
		return nil, fmt.Errorf("cannot map double value to %s", kind)
	}
}

func enumeratedValue(
	column profile.Column,
	kind kublingv1.ValueType,
	value uint32,
) (*kublingv1.Value, error) {
	switch kind {
	case kublingv1.ValueType_VALUE_TYPE_STRING:
		if label, exists := column.Enum[value]; exists {
			return stringValue(label), nil
		}
		return stringValue(strconv.FormatUint(uint64(value), 10)), nil
	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		if uint64(value) > math.MaxInt32 {
			return nil, fmt.Errorf("enumerated value %d exceeds INTEGER", value)
		}
		return integerValue(int32(value)), nil
	case kublingv1.ValueType_VALUE_TYPE_LONG:
		return longValue(int64(value)), nil
	default:
		return nil, fmt.Errorf("cannot map enumerated value to %s", kind)
	}
}

func formatBitString(value bacnet.BitString) string {
	bitCount := len(value.Bytes)*8 - int(value.UnusedBits)
	if bitCount < 0 {
		bitCount = 0
	}
	var result strings.Builder
	result.Grow(bitCount)
	for index := 0; index < bitCount; index++ {
		mask := byte(1 << (7 - uint(index%8)))
		if value.Bytes[index/8]&mask == 0 {
			result.WriteByte('0')
		} else {
			result.WriteByte('1')
		}
	}
	return result.String()
}

func formatDate(value bacnet.Date, allowWildcards bool) (string, error) {
	if value.Year == 0xff || value.Month == 0xff || value.Day == 0xff {
		if allowWildcards {
			return fmt.Sprintf("%s-%s-%s", datePart(value.Year, true), datePart(value.Month, false), datePart(value.Day, false)), nil
		}
		return "", fmt.Errorf("wildcard BACnet date cannot be represented as DATE")
	}
	year := int(value.Year) + 1900
	if value.Month < 1 || value.Month > 12 || value.Day < 1 || value.Day > 31 {
		if allowWildcards {
			return fmt.Sprintf("%04d-%02d-%02d", year, value.Month, value.Day), nil
		}
		return "", fmt.Errorf("BACnet date contains a special month or day value")
	}
	parsed := time.Date(year, time.Month(value.Month), int(value.Day), 0, 0, 0, 0, time.UTC)
	if parsed.Year() != year || int(parsed.Month()) != int(value.Month) || parsed.Day() != int(value.Day) {
		return "", fmt.Errorf("invalid BACnet date")
	}
	return parsed.Format("2006-01-02"), nil
}

func datePart(value uint8, year bool) string {
	if value == 0xff {
		if year {
			return "****"
		}
		return "**"
	}
	if year {
		return fmt.Sprintf("%04d", int(value)+1900)
	}
	return fmt.Sprintf("%02d", value)
}

func formatTime(value bacnet.Time, allowWildcards bool) (string, error) {
	if value.Hour == 0xff || value.Minute == 0xff || value.Second == 0xff || value.Hundredths == 0xff {
		if allowWildcards {
			return fmt.Sprintf(
				"%s:%s:%s.%s",
				timePart(value.Hour),
				timePart(value.Minute),
				timePart(value.Second),
				timePart(value.Hundredths),
			), nil
		}
		return "", fmt.Errorf("wildcard BACnet time cannot be represented as TIME")
	}
	if value.Hour > 23 || value.Minute > 59 || value.Second > 59 || value.Hundredths > 99 {
		return "", fmt.Errorf("invalid BACnet time")
	}
	base := fmt.Sprintf("%02d:%02d:%02d", value.Hour, value.Minute, value.Second)
	if value.Hundredths != 0 {
		base += fmt.Sprintf(".%02d", value.Hundredths)
	}
	return base, nil
}

func timePart(value uint8) string {
	if value == 0xff {
		return "**"
	}
	return fmt.Sprintf("%02d", value)
}

func formatObjectIdentifier(selected *profile.Profile, identifier bacnet.ObjectIdentifier) string {
	return fmt.Sprintf("%s:%d", objectTypeName(selected, identifier.Type), identifier.Instance)
}

func objectTypeName(selected *profile.Profile, objectType bacnet.ObjectType) string {
	if selected != nil {
		for _, candidate := range selected.ObjectTypes {
			if candidate.ID == uint32(objectType) {
				return candidate.Name
			}
		}
	}
	return strconv.FormatUint(uint64(objectType), 10)
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func nullableStringValue(value string) *kublingv1.Value {
	if value == "" {
		return nullValue()
	}
	return stringValue(value)
}

func varbinaryValue(value []byte) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_VarbinaryValue{
		VarbinaryValue: append([]byte(nil), value...),
	}}
}

func booleanValue(value bool) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BooleanValue{BooleanValue: value}}
}

func integerValue(value int32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: value}}
}

func nullableUnsignedIntegerValue(value uint32) *kublingv1.Value {
	if value == 0 {
		return nullValue()
	}
	return integerValue(int32(value))
}

func longValue(value int64) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_LongValue{LongValue: value}}
}

func timestampValue(value time.Time) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_TimestampValue{
		TimestampValue: value.UTC().Format("2006-01-02T15:04:05.999999999"),
	}}
}

func nullableTimestampValue(value time.Time) *kublingv1.Value {
	if value.IsZero() {
		return nullValue()
	}
	return timestampValue(value)
}

func nullValue() *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_NullValue{NullValue: &kublingv1.NullValue{}}}
}
