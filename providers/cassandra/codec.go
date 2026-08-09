package cassandra

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"gopkg.in/inf.v0"
)

func nativeToValue(
	typeInfo gocql.TypeInfo,
	value any,
) (*kublingv1.Value, error) {
	if value == nil {
		return nullProviderValue(), nil
	}
	if typeInfo == nil {
		return nil, fmt.Errorf("Cassandra type metadata is required")
	}

	switch typeInfo.Type() {
	case gocql.TypeAscii, gocql.TypeText, gocql.TypeVarchar,
		gocql.TypeUUID, gocql.TypeTimeUUID, gocql.TypeInet:
		return stringProviderValue(fmt.Sprint(value)), nil
	case gocql.TypeDuration:
		if duration, ok := value.(gocql.Duration); ok {
			return stringProviderValue(fmt.Sprintf(
				"%dmo%dd%dns",
				duration.Months,
				duration.Days,
				duration.Nanoseconds,
			)), nil
		}
		return stringProviderValue(fmt.Sprint(value)), nil
	case gocql.TypeBigInt, gocql.TypeCounter:
		converted, err := integerValue(value, 64)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_LongValue{
			LongValue: converted,
		}}, nil
	case gocql.TypeBlob:
		data, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("convert Cassandra blob %T", value)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_BlobValue{
			BlobValue: &kublingv1.BlobValue{Data: append([]byte(nil), data...)},
		}}, nil
	case gocql.TypeBoolean:
		converted, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("convert Cassandra boolean %T", value)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_BooleanValue{
			BooleanValue: converted,
		}}, nil
	case gocql.TypeDecimal:
		return &kublingv1.Value{Kind: &kublingv1.Value_BigdecimalValue{
			BigdecimalValue: fmt.Sprint(value),
		}}, nil
	case gocql.TypeDouble:
		converted, err := floatingValue(value)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_DoubleValue{
			DoubleValue: converted,
		}}, nil
	case gocql.TypeFloat:
		converted, err := floatingValue(value)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_FloatValue{
			FloatValue: float32(converted),
		}}, nil
	case gocql.TypeInt:
		converted, err := integerValue(value, 32)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{
			IntegerValue: int32(converted),
		}}, nil
	case gocql.TypeVarint:
		return &kublingv1.Value{Kind: &kublingv1.Value_BigintegerValue{
			BigintegerValue: fmt.Sprint(value),
		}}, nil
	case gocql.TypeSmallInt:
		converted, err := integerValue(value, 16)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_ShortValue{
			ShortValue: int32(converted),
		}}, nil
	case gocql.TypeTinyInt:
		converted, err := integerValue(value, 8)
		if err != nil {
			return nil, err
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_ByteValue{
			ByteValue: int32(converted),
		}}, nil
	case gocql.TypeTimestamp:
		converted, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("convert Cassandra timestamp %T", value)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_TimestampValue{
			TimestampValue: converted.Format("2006-01-02T15:04:05.999999999"),
		}}, nil
	case gocql.TypeDate:
		converted, ok := value.(time.Time)
		if !ok {
			return nil, fmt.Errorf("convert Cassandra date %T", value)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_DateValue{
			DateValue: converted.Format("2006-01-02"),
		}}, nil
	case gocql.TypeTime:
		converted, ok := value.(time.Duration)
		if !ok {
			return nil, fmt.Errorf("convert Cassandra time %T", value)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_TimeValue{
			TimeValue: formatLocalTime(converted),
		}}, nil
	case gocql.TypeList, gocql.TypeMap, gocql.TypeSet,
		gocql.TypeUDT, gocql.TypeTuple, gocql.TypeCustom:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode Cassandra %s as JSON: %w", nativeTypeName(typeInfo), err)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_JsonValue{
			JsonValue: string(encoded),
		}}, nil
	default:
		return nil, fmt.Errorf("unsupported Cassandra type %s", nativeTypeName(typeInfo))
	}
}

func providerToNative(
	value *kublingv1.Value,
	typeInfo gocql.TypeInfo,
) (any, error) {
	if value == nil || value.GetNullValue() != nil {
		return nil, nil
	}
	if typeInfo == nil {
		return nil, fmt.Errorf("Cassandra type metadata is required")
	}

	switch typeInfo.Type() {
	case gocql.TypeAscii, gocql.TypeText, gocql.TypeVarchar:
		return providerString(value)
	case gocql.TypeUUID, gocql.TypeTimeUUID:
		raw, err := providerString(value)
		if err != nil {
			return nil, err
		}
		return gocql.ParseUUID(raw)
	case gocql.TypeInet:
		raw, err := providerString(value)
		if err != nil {
			return nil, err
		}
		address := net.ParseIP(raw)
		if address == nil {
			return nil, fmt.Errorf("invalid inet value %q", raw)
		}
		return address, nil
	case gocql.TypeDuration:
		raw, err := providerString(value)
		if err != nil {
			return nil, err
		}
		return parseCassandraDuration(raw)
	case gocql.TypeBigInt, gocql.TypeCounter:
		return providerInteger(value, 64)
	case gocql.TypeBlob:
		return providerBytes(value)
	case gocql.TypeBoolean:
		if converted, ok := value.GetKind().(*kublingv1.Value_BooleanValue); ok {
			return converted.BooleanValue, nil
		}
		return nil, fmt.Errorf("expected boolean value")
	case gocql.TypeDecimal:
		raw, err := providerNumberString(value)
		if err != nil {
			return nil, err
		}
		converted := new(inf.Dec)
		if _, ok := converted.SetString(raw); !ok {
			return nil, fmt.Errorf("invalid decimal value %q", raw)
		}
		return converted, nil
	case gocql.TypeDouble:
		return providerFloat(value, 64)
	case gocql.TypeFloat:
		converted, err := providerFloat(value, 32)
		return float32(converted), err
	case gocql.TypeInt:
		converted, err := providerInteger(value, 32)
		return int32(converted), err
	case gocql.TypeVarint:
		raw, err := providerNumberString(value)
		if err != nil {
			return nil, err
		}
		converted, ok := new(big.Int).SetString(raw, 10)
		if !ok {
			return nil, fmt.Errorf("invalid varint value %q", raw)
		}
		return converted, nil
	case gocql.TypeSmallInt:
		converted, err := providerInteger(value, 16)
		return int16(converted), err
	case gocql.TypeTinyInt:
		converted, err := providerInteger(value, 8)
		return int8(converted), err
	case gocql.TypeTimestamp:
		raw, err := providerString(value)
		if err != nil {
			return nil, err
		}
		return parseTimestamp(raw)
	case gocql.TypeDate:
		raw, err := providerString(value)
		if err != nil {
			return nil, err
		}
		return time.Parse("2006-01-02", raw)
	case gocql.TypeTime:
		raw, err := providerString(value)
		if err != nil {
			return nil, err
		}
		return parseLocalTime(raw)
	case gocql.TypeList, gocql.TypeMap, gocql.TypeSet,
		gocql.TypeUDT, gocql.TypeTuple, gocql.TypeCustom:
		raw, err := providerString(value)
		if err != nil {
			return nil, err
		}
		zero := typeInfo.Zero()
		zeroType := reflect.TypeOf(zero)
		if zeroType == nil {
			var decoded any
			if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
				return nil, fmt.Errorf("decode JSON for Cassandra %s: %w", nativeTypeName(typeInfo), err)
			}
			return decoded, nil
		}
		target := reflect.New(zeroType)
		if err := json.Unmarshal([]byte(raw), target.Interface()); err != nil {
			return nil, fmt.Errorf("decode JSON for Cassandra %s: %w", nativeTypeName(typeInfo), err)
		}
		return target.Elem().Interface(), nil
	default:
		return nil, fmt.Errorf("unsupported Cassandra type %s", nativeTypeName(typeInfo))
	}
}

func providerString(value *kublingv1.Value) (string, error) {
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_StringValue:
		return typed.StringValue, nil
	case *kublingv1.Value_CharValue:
		return typed.CharValue, nil
	case *kublingv1.Value_ClobValue:
		return typed.ClobValue.GetData(), nil
	case *kublingv1.Value_XmlValue:
		return typed.XmlValue, nil
	case *kublingv1.Value_JsonValue:
		return typed.JsonValue, nil
	case *kublingv1.Value_DateValue:
		return typed.DateValue, nil
	case *kublingv1.Value_TimeValue:
		return typed.TimeValue, nil
	case *kublingv1.Value_TimestampValue:
		return typed.TimestampValue, nil
	default:
		return "", fmt.Errorf("expected character value, got %T", value.GetKind())
	}
}

func providerBytes(value *kublingv1.Value) ([]byte, error) {
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_BlobValue:
		return append([]byte(nil), typed.BlobValue.GetData()...), nil
	case *kublingv1.Value_VarbinaryValue:
		return append([]byte(nil), typed.VarbinaryValue...), nil
	case *kublingv1.Value_GeometryValue:
		return append([]byte(nil), typed.GeometryValue...), nil
	case *kublingv1.Value_GeographyValue:
		return append([]byte(nil), typed.GeographyValue...), nil
	default:
		return nil, fmt.Errorf("expected binary value, got %T", value.GetKind())
	}
}

func providerInteger(value *kublingv1.Value, bits int) (int64, error) {
	var converted int64
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_ByteValue:
		converted = int64(typed.ByteValue)
	case *kublingv1.Value_ShortValue:
		converted = int64(typed.ShortValue)
	case *kublingv1.Value_IntegerValue:
		converted = int64(typed.IntegerValue)
	case *kublingv1.Value_LongValue:
		converted = typed.LongValue
	case *kublingv1.Value_BigintegerValue:
		parsed, err := strconv.ParseInt(typed.BigintegerValue, 10, bits)
		if err != nil {
			return 0, err
		}
		converted = parsed
	default:
		return 0, fmt.Errorf("expected integer value, got %T", value.GetKind())
	}
	if bits < 64 {
		minimum := -(int64(1) << (bits - 1))
		maximum := (int64(1) << (bits - 1)) - 1
		if converted < minimum || converted > maximum {
			return 0, fmt.Errorf("integer %d is outside signed %d-bit range", converted, bits)
		}
	}

	return converted, nil
}

func providerFloat(value *kublingv1.Value, bits int) (float64, error) {
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_FloatValue:
		return float64(typed.FloatValue), nil
	case *kublingv1.Value_DoubleValue:
		return typed.DoubleValue, nil
	case *kublingv1.Value_BigdecimalValue:
		return strconv.ParseFloat(typed.BigdecimalValue, bits)
	default:
		return 0, fmt.Errorf("expected floating-point value, got %T", value.GetKind())
	}
}

func providerNumberString(value *kublingv1.Value) (string, error) {
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_ByteValue:
		return strconv.FormatInt(int64(typed.ByteValue), 10), nil
	case *kublingv1.Value_ShortValue:
		return strconv.FormatInt(int64(typed.ShortValue), 10), nil
	case *kublingv1.Value_IntegerValue:
		return strconv.FormatInt(int64(typed.IntegerValue), 10), nil
	case *kublingv1.Value_LongValue:
		return strconv.FormatInt(typed.LongValue, 10), nil
	case *kublingv1.Value_BigintegerValue:
		return typed.BigintegerValue, nil
	case *kublingv1.Value_FloatValue:
		return strconv.FormatFloat(float64(typed.FloatValue), 'g', -1, 32), nil
	case *kublingv1.Value_DoubleValue:
		return strconv.FormatFloat(typed.DoubleValue, 'g', -1, 64), nil
	case *kublingv1.Value_BigdecimalValue:
		return typed.BigdecimalValue, nil
	default:
		return "", fmt.Errorf("expected numeric value, got %T", value.GetKind())
	}
}

func integerValue(value any, bits int) (int64, error) {
	converted, err := strconv.ParseInt(fmt.Sprint(value), 10, bits)
	if err != nil {
		return 0, fmt.Errorf("convert Cassandra integer %T: %w", value, err)
	}
	return converted, nil
}

func floatingValue(value any) (float64, error) {
	converted, err := strconv.ParseFloat(fmt.Sprint(value), 64)
	if err != nil {
		return 0, fmt.Errorf("convert Cassandra floating-point %T: %w", value, err)
	}
	return converted, nil
}

func nullProviderValue() *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_NullValue{
		NullValue: &kublingv1.NullValue{},
	}}
}

func stringProviderValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{
		StringValue: value,
	}}
}

func parseTimestamp(value string) (time.Time, error) {
	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02 15:04:05.999999999",
	} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid timestamp value %q", value)
}

func parseLocalTime(value string) (time.Duration, error) {
	parsed, err := time.Parse("15:04:05.999999999", value)
	if err != nil {
		return 0, fmt.Errorf("invalid time value %q: %w", value, err)
	}

	return time.Duration(parsed.Hour())*time.Hour +
		time.Duration(parsed.Minute())*time.Minute +
		time.Duration(parsed.Second())*time.Second +
		time.Duration(parsed.Nanosecond()), nil
}

func parseCassandraDuration(value string) (any, error) {
	var months int32
	var days int32
	var nanoseconds int64
	if count, err := fmt.Sscanf(
		value,
		"%dmo%dd%dns",
		&months,
		&days,
		&nanoseconds,
	); err == nil && count == 3 {
		return gocql.Duration{
			Months:      months,
			Days:        days,
			Nanoseconds: nanoseconds,
		}, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return nil, fmt.Errorf("invalid Cassandra duration value %q: %w", value, err)
	}
	return duration, nil
}

func formatLocalTime(value time.Duration) string {
	if value < 0 {
		return value.String()
	}
	hours := value / time.Hour
	value -= hours * time.Hour
	minutes := value / time.Minute
	value -= minutes * time.Minute
	seconds := value / time.Second
	nanoseconds := value - seconds*time.Second

	result := fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds)
	if nanoseconds == 0 {
		return result
	}

	return result + strings.TrimRight(fmt.Sprintf(".%09d", nanoseconds), "0")
}
