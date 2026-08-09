package redis

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
)

func decodeRedisValue(raw string, valueType kublingv1.ValueType) (*kublingv1.Value, error) {
	switch valueType {
	case kublingv1.ValueType_VALUE_TYPE_STRING:
		return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: raw}}, nil
	case kublingv1.ValueType_VALUE_TYPE_VARBINARY:
		return &kublingv1.Value{Kind: &kublingv1.Value_VarbinaryValue{VarbinaryValue: []byte(raw)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_CHAR:
		return &kublingv1.Value{Kind: &kublingv1.Value_CharValue{CharValue: raw}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BOOLEAN:
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_BooleanValue{BooleanValue: parsed}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BYTE:
		parsed, err := strconv.ParseInt(raw, 10, 8)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_ByteValue{ByteValue: int32(parsed)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_SHORT:
		parsed, err := strconv.ParseInt(raw, 10, 16)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_ShortValue{ShortValue: int32(parsed)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		parsed, err := strconv.ParseInt(raw, 10, 32)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: int32(parsed)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_LONG:
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_LongValue{LongValue: parsed}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BIGINTEGER:
		if _, ok := newBigInteger(raw); !ok {
			return nil, fmt.Errorf("invalid biginteger %q", raw)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_BigintegerValue{BigintegerValue: raw}}, nil
	case kublingv1.ValueType_VALUE_TYPE_FLOAT:
		parsed, err := strconv.ParseFloat(raw, 32)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_FloatValue{FloatValue: float32(parsed)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_DoubleValue{DoubleValue: parsed}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL:
		if _, ok := newBigDecimal(raw); !ok {
			return nil, fmt.Errorf("invalid bigdecimal %q", raw)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_BigdecimalValue{BigdecimalValue: raw}}, nil
	case kublingv1.ValueType_VALUE_TYPE_DATE:
		return &kublingv1.Value{Kind: &kublingv1.Value_DateValue{DateValue: raw}}, nil
	case kublingv1.ValueType_VALUE_TYPE_TIME:
		return &kublingv1.Value{Kind: &kublingv1.Value_TimeValue{TimeValue: raw}}, nil
	case kublingv1.ValueType_VALUE_TYPE_TIMESTAMP:
		return &kublingv1.Value{Kind: &kublingv1.Value_TimestampValue{TimestampValue: raw}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BLOB:
		return &kublingv1.Value{Kind: &kublingv1.Value_BlobValue{BlobValue: &kublingv1.BlobValue{Data: []byte(raw)}}}, nil
	case kublingv1.ValueType_VALUE_TYPE_CLOB:
		return &kublingv1.Value{Kind: &kublingv1.Value_ClobValue{ClobValue: &kublingv1.ClobValue{Data: raw}}}, nil
	case kublingv1.ValueType_VALUE_TYPE_GEOMETRY:
		return &kublingv1.Value{Kind: &kublingv1.Value_GeometryValue{GeometryValue: []byte(raw)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_GEOGRAPHY:
		return &kublingv1.Value{Kind: &kublingv1.Value_GeographyValue{GeographyValue: []byte(raw)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_JSON:
		return &kublingv1.Value{Kind: &kublingv1.Value_JsonValue{JsonValue: raw}}, nil
	case kublingv1.ValueType_VALUE_TYPE_XML:
		return &kublingv1.Value{Kind: &kublingv1.Value_XmlValue{XmlValue: raw}}, nil
	default:
		return nil, fmt.Errorf("unsupported Redis value type %s", valueType)
	}
}

func encodeRedisValue(value *kublingv1.Value, expected kublingv1.ValueType) (string, bool, error) {
	if value == nil || value.GetNullValue() != nil {
		return "", true, nil
	}
	var raw string
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_StringValue:
		raw = typed.StringValue
	case *kublingv1.Value_VarbinaryValue:
		raw = string(typed.VarbinaryValue)
	case *kublingv1.Value_CharValue:
		raw = typed.CharValue
	case *kublingv1.Value_BooleanValue:
		raw = strconv.FormatBool(typed.BooleanValue)
	case *kublingv1.Value_ByteValue:
		raw = strconv.FormatInt(int64(typed.ByteValue), 10)
	case *kublingv1.Value_ShortValue:
		raw = strconv.FormatInt(int64(typed.ShortValue), 10)
	case *kublingv1.Value_IntegerValue:
		raw = strconv.FormatInt(int64(typed.IntegerValue), 10)
	case *kublingv1.Value_LongValue:
		raw = strconv.FormatInt(typed.LongValue, 10)
	case *kublingv1.Value_BigintegerValue:
		raw = typed.BigintegerValue
	case *kublingv1.Value_FloatValue:
		raw = strconv.FormatFloat(float64(typed.FloatValue), 'g', -1, 32)
	case *kublingv1.Value_DoubleValue:
		raw = strconv.FormatFloat(typed.DoubleValue, 'g', -1, 64)
	case *kublingv1.Value_BigdecimalValue:
		raw = typed.BigdecimalValue
	case *kublingv1.Value_DateValue:
		raw = typed.DateValue
	case *kublingv1.Value_TimeValue:
		raw = typed.TimeValue
	case *kublingv1.Value_TimestampValue:
		raw = typed.TimestampValue
	case *kublingv1.Value_BlobValue:
		raw = string(typed.BlobValue.GetData())
	case *kublingv1.Value_ClobValue:
		raw = typed.ClobValue.GetData()
	case *kublingv1.Value_GeometryValue:
		raw = string(typed.GeometryValue)
	case *kublingv1.Value_GeographyValue:
		raw = string(typed.GeographyValue)
	case *kublingv1.Value_JsonValue:
		raw = typed.JsonValue
	case *kublingv1.Value_XmlValue:
		raw = typed.XmlValue
	default:
		return "", false, fmt.Errorf("unsupported value %T", value.GetKind())
	}
	if _, err := decodeRedisValue(raw, expected); err != nil {
		return "", false, err
	}
	return raw, false, nil
}

func nullValue() *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_NullValue{NullValue: &kublingv1.NullValue{}}}
}

func newBigInteger(value string) (string, bool) {
	if value == "" {
		return "", false
	}
	start := 0
	if value[0] == '-' || value[0] == '+' {
		start = 1
	}
	if start == len(value) {
		return "", false
	}
	for _, character := range value[start:] {
		if character < '0' || character > '9' {
			return "", false
		}
	}
	return value, true
}

func newBigDecimal(value string) (string, bool) {
	if strings.TrimSpace(value) != value || value == "" {
		return "", false
	}
	_, ok := new(big.Rat).SetString(value)
	return value, ok
}
