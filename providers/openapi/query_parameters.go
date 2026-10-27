package openapi

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"net/url"
	"sort"
	"strconv"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/protobuf/proto"
)

type requestBindings struct {
	endpoint string
	path     map[string]string
	query    url.Values
	columns  map[string]*kublingv1.Value
}

func planQueryBindings(descriptor *entityDescriptor, filter *providerv1.Expression) (*requestBindings, error) {
	bindings := &requestBindings{
		path:    make(map[string]string, len(descriptor.config.PathParameters)),
		query:   make(url.Values, len(descriptor.config.QueryParameters)+len(descriptor.config.EqualityFilters)),
		columns: make(map[string]*kublingv1.Value, len(descriptor.parameterBindings)),
	}
	for _, configured := range descriptor.config.PathParameters {
		if configured.Field == "" {
			bindings.path[configured.Parameter] = configured.Value
		}
	}
	for _, configured := range descriptor.config.QueryParameters {
		bindings.query.Set(configured.Name, configured.Value)
	}

	filterValues := make(map[string]*kublingv1.Value)
	if filter != nil {
		var err error
		filterValues, err = filterLiteralValues(filter)
		if err != nil {
			return nil, err
		}
	}

	columns := make(map[string]*providerv1.ColumnMetadata, len(descriptor.table.GetColumns()))
	for _, column := range descriptor.table.GetColumns() {
		columns[strings.ToUpper(column.GetName())] = column
	}
	serializedValues := make(map[string]string, len(filterValues))
	usedFields := make(map[string]struct{}, len(filterValues))
	for _, binding := range descriptor.parameterBindings {
		field := strings.ToUpper(binding.field)
		literal := filterValues[field]
		if literal == nil {
			if binding.required {
				return nil, fmt.Errorf(
					"required %s parameter %q requires an equality filter on field %q",
					binding.location,
					binding.parameter,
					binding.field,
				)
			}
			continue
		}
		serialized, exists := serializedValues[field]
		if !exists {
			var err error
			serialized, err = requestParameterLiteralValue(literal)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", binding.field, err)
			}
			column := columns[field]
			if column == nil {
				return nil, fmt.Errorf("binding field %q is not present in table metadata", binding.field)
			}
			columnValue, err := requestBindingColumnValue(serialized, column)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", binding.field, err)
			}
			serializedValues[field] = serialized
			bindings.columns[field] = columnValue
		}
		usedFields[field] = struct{}{}
		switch binding.location {
		case parameterLocationPath:
			if err := setPathBinding(bindings.path, binding.parameter, serialized); err != nil {
				return nil, err
			}
		case parameterLocationQuery:
			if err := setQueryBinding(bindings.query, binding.parameter, serialized); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("parameter %q has unsupported binding location %q", binding.parameter, binding.location)
		}
	}

	unknownFields := make([]string, 0)
	for field := range filterValues {
		if _, used := usedFields[field]; !used {
			unknownFields = append(unknownFields, field)
		}
	}
	if len(unknownFields) > 0 {
		sort.Strings(unknownFields)
		return nil, fmt.Errorf("field %q has no request parameter binding", unknownFields[0])
	}
	return bindings, nil
}

func setPathBinding(parameters map[string]string, name, value string) error {
	if existing, exists := parameters[name]; exists && existing != value {
		return fmt.Errorf("path parameter %q has conflicting binding values", name)
	}
	parameters[name] = value
	return nil
}

func setQueryBinding(parameters url.Values, name, value string) error {
	if existing, exists := parameters[name]; exists {
		if len(existing) != 1 || existing[0] != value {
			return fmt.Errorf("query parameter %q has conflicting binding values", name)
		}
		return nil
	}
	parameters.Set(name, value)
	return nil
}

func requestBindingColumnValue(serialized string, column *providerv1.ColumnMetadata) (*kublingv1.Value, error) {
	switch column.GetType() {
	case kublingv1.ValueType_VALUE_TYPE_STRING:
		return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: serialized}}, nil
	case kublingv1.ValueType_VALUE_TYPE_VARBINARY:
		decoded, err := base64.StdEncoding.DecodeString(serialized)
		if err != nil {
			return nil, fmt.Errorf("decode binary request parameter: %w", err)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_VarbinaryValue{VarbinaryValue: decoded}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BOOLEAN:
		value, err := strconv.ParseBool(serialized)
		if err != nil {
			return nil, fmt.Errorf("decode boolean request parameter: %w", err)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_BooleanValue{BooleanValue: value}}, nil
	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		value, err := strconv.ParseInt(serialized, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("decode integer request parameter: %w", err)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: int32(value)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_LONG:
		value, err := strconv.ParseInt(serialized, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("decode long request parameter: %w", err)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_LongValue{LongValue: value}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BIGINTEGER:
		if _, ok := new(big.Int).SetString(serialized, 10); !ok {
			return nil, fmt.Errorf("decode big integer request parameter %q", serialized)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_BigintegerValue{BigintegerValue: serialized}}, nil
	case kublingv1.ValueType_VALUE_TYPE_FLOAT:
		value, err := strconv.ParseFloat(serialized, 32)
		if err != nil {
			return nil, fmt.Errorf("decode float request parameter: %w", err)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_FloatValue{FloatValue: float32(value)}}, nil
	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		value, err := strconv.ParseFloat(serialized, 64)
		if err != nil {
			return nil, fmt.Errorf("decode double request parameter: %w", err)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_DoubleValue{DoubleValue: value}}, nil
	case kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL:
		if _, ok := new(big.Float).SetString(serialized); !ok {
			return nil, fmt.Errorf("decode decimal request parameter %q", serialized)
		}
		return &kublingv1.Value{Kind: &kublingv1.Value_BigdecimalValue{BigdecimalValue: serialized}}, nil
	case kublingv1.ValueType_VALUE_TYPE_DATE:
		return &kublingv1.Value{Kind: &kublingv1.Value_DateValue{DateValue: serialized}}, nil
	case kublingv1.ValueType_VALUE_TYPE_TIME:
		return &kublingv1.Value{Kind: &kublingv1.Value_TimeValue{TimeValue: serialized}}, nil
	case kublingv1.ValueType_VALUE_TYPE_TIMESTAMP:
		return &kublingv1.Value{Kind: &kublingv1.Value_TimestampValue{TimestampValue: serialized}}, nil
	default:
		return nil, fmt.Errorf("column %q has unsupported request parameter type %s", column.GetName(), column.GetType())
	}
}

func cloneRequestBindings(bindings *requestBindings) *requestBindings {
	if bindings == nil {
		return nil
	}
	cloned := &requestBindings{
		endpoint: bindings.endpoint,
		path:     make(map[string]string, len(bindings.path)),
		query:    cloneURLValues(bindings.query),
		columns:  make(map[string]*kublingv1.Value, len(bindings.columns)),
	}
	for name, value := range bindings.path {
		cloned.path[name] = value
	}
	for name, value := range bindings.columns {
		cloned.columns[name] = proto.Clone(value).(*kublingv1.Value)
	}
	return cloned
}

func cloneURLValues(values url.Values) url.Values {
	cloned := make(url.Values, len(values))
	for name, candidates := range values {
		cloned[name] = append([]string(nil), candidates...)
	}
	return cloned
}

func requestParameterLiteralValue(value *kublingv1.Value) (string, error) {
	if value == nil {
		return "", fmt.Errorf("literal value is required")
	}
	switch kind := value.GetKind().(type) {
	case *kublingv1.Value_StringValue:
		return kind.StringValue, nil
	case *kublingv1.Value_CharValue:
		return kind.CharValue, nil
	case *kublingv1.Value_BooleanValue:
		return strconv.FormatBool(kind.BooleanValue), nil
	case *kublingv1.Value_ByteValue:
		return strconv.FormatInt(int64(kind.ByteValue), 10), nil
	case *kublingv1.Value_ShortValue:
		return strconv.FormatInt(int64(kind.ShortValue), 10), nil
	case *kublingv1.Value_IntegerValue:
		return strconv.FormatInt(int64(kind.IntegerValue), 10), nil
	case *kublingv1.Value_LongValue:
		return strconv.FormatInt(kind.LongValue, 10), nil
	case *kublingv1.Value_BigintegerValue:
		return kind.BigintegerValue, nil
	case *kublingv1.Value_FloatValue:
		return strconv.FormatFloat(float64(kind.FloatValue), 'g', -1, 32), nil
	case *kublingv1.Value_DoubleValue:
		return strconv.FormatFloat(kind.DoubleValue, 'g', -1, 64), nil
	case *kublingv1.Value_BigdecimalValue:
		return kind.BigdecimalValue, nil
	case *kublingv1.Value_DateValue:
		return kind.DateValue, nil
	case *kublingv1.Value_TimeValue:
		return kind.TimeValue, nil
	case *kublingv1.Value_TimestampValue:
		return kind.TimestampValue, nil
	case *kublingv1.Value_VarbinaryValue:
		return base64.StdEncoding.EncodeToString(kind.VarbinaryValue), nil
	case *kublingv1.Value_NullValue:
		return "", fmt.Errorf("null literals cannot be encoded as request parameters")
	default:
		return "", fmt.Errorf("literal type %T cannot be encoded as a request parameter", kind)
	}
}
