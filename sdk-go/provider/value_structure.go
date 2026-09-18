package provider

import (
	"fmt"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/protobuf/proto"
)

func validateTypeDescriptor(descriptor *kublingv1.TypeDescriptor) error {
	return validateTypeDescriptorPath(
		descriptor,
		make(map[*kublingv1.TypeDescriptor]struct{}),
	)
}

func validateTypeDescriptorPath(
	descriptor *kublingv1.TypeDescriptor,
	path map[*kublingv1.TypeDescriptor]struct{},
) error {
	if descriptor == nil {
		return fmt.Errorf("type descriptor is required")
	}
	if _, cyclic := path[descriptor]; cyclic {
		return fmt.Errorf("type descriptor contains a cycle")
	}
	path[descriptor] = struct{}{}
	defer delete(path, descriptor)

	valueType := descriptor.GetType()
	if valueType == kublingv1.ValueType_VALUE_TYPE_UNKNOWN {
		return fmt.Errorf("type descriptor must not use VALUE_TYPE_UNKNOWN")
	}
	if _, known := kublingv1.ValueType_name[int32(valueType)]; !known {
		return fmt.Errorf("type descriptor uses unknown type %d", valueType)
	}

	if valueType == kublingv1.ValueType_VALUE_TYPE_ARRAY {
		if descriptor.GetElementType() == nil {
			return fmt.Errorf("ARRAY type descriptor requires element_type")
		}
		if err := validateTypeDescriptorPath(descriptor.GetElementType(), path); err != nil {
			return fmt.Errorf("ARRAY element_type: %w", err)
		}
	} else if descriptor.GetElementType() != nil {
		return fmt.Errorf("%s type descriptor must not set element_type", valueType)
	}

	if descriptor.Precision != nil {
		if descriptor.GetPrecision() <= 0 {
			return fmt.Errorf("type descriptor precision must be positive")
		}
		if valueType != kublingv1.ValueType_VALUE_TYPE_BIGINTEGER &&
			valueType != kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL {
			return fmt.Errorf(
				"%s type descriptor must not set precision",
				valueType,
			)
		}
	}

	if descriptor.Scale != nil {
		if valueType != kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL {
			return fmt.Errorf(
				"%s type descriptor must not set scale",
				valueType,
			)
		}
		if descriptor.Precision == nil {
			return fmt.Errorf("BIGDECIMAL scale requires precision")
		}
		if descriptor.GetScale() > descriptor.GetPrecision() {
			return fmt.Errorf("BIGDECIMAL scale must not exceed precision")
		}
	}

	return nil
}

func validateOutputBatchStructure(batch *providerv1.TupleBatch) error {
	if batch == nil {
		return nil
	}

	descriptors := make(
		[]*kublingv1.TypeDescriptor,
		len(batch.GetFields()),
	)
	for fieldIndex, field := range batch.GetFields() {
		if field == nil {
			return fmt.Errorf("field %d is nil", fieldIndex)
		}

		valueType := field.GetType()
		if valueType == kublingv1.ValueType_VALUE_TYPE_UNKNOWN {
			return fmt.Errorf("field %d type is VALUE_TYPE_UNKNOWN", fieldIndex)
		}
		if _, known := kublingv1.ValueType_name[int32(valueType)]; !known {
			return fmt.Errorf("field %d type %d is unknown", fieldIndex, valueType)
		}

		descriptor := field.GetTypeDescriptor()
		if descriptor == nil {
			if valueType == kublingv1.ValueType_VALUE_TYPE_ARRAY {
				return fmt.Errorf("field %d ARRAY requires type_descriptor", fieldIndex)
			}
			descriptor = &kublingv1.TypeDescriptor{Type: valueType}
		} else {
			if err := validateTypeDescriptor(descriptor); err != nil {
				return fmt.Errorf("field %d type_descriptor: %w", fieldIndex, err)
			}
			if descriptor.GetType() != valueType {
				return fmt.Errorf(
					"field %d type %s does not match type_descriptor %s",
					fieldIndex,
					valueType,
					descriptor.GetType(),
				)
			}
		}
		descriptors[fieldIndex] = descriptor
	}

	return validateBatchTuples(batch, descriptors)
}

func validateInputBatchStructure(batch *providerv1.TupleBatch) error {
	if batch == nil {
		// Request presence and provider-specific mutation requirements remain the
		// provider's responsibility. The SDK validates values when rows exist.
		return nil
	}

	descriptors := make(
		[]*kublingv1.TypeDescriptor,
		len(batch.GetFields()),
	)
	for fieldIndex, field := range batch.GetFields() {
		if field == nil {
			return fmt.Errorf("field %d is nil", fieldIndex)
		}

		descriptor := field.GetTypeDescriptor()
		if descriptor != nil {
			if err := validateTypeDescriptor(descriptor); err != nil {
				return fmt.Errorf("field %d type_descriptor: %w", fieldIndex, err)
			}
			if descriptor.GetType() != field.GetType() {
				return fmt.Errorf(
					"field %d type %s does not match type_descriptor %s",
					fieldIndex,
					field.GetType(),
					descriptor.GetType(),
				)
			}
			descriptors[fieldIndex] = descriptor
			continue
		}

		valueType := field.GetType()
		if valueType == kublingv1.ValueType_VALUE_TYPE_UNKNOWN {
			// Legacy insert requests identify fields by name and let the provider
			// resolve their declared types from source metadata.
			continue
		}
		if _, known := kublingv1.ValueType_name[int32(valueType)]; !known {
			return fmt.Errorf("field %d type %d is unknown", fieldIndex, valueType)
		}
		if valueType == kublingv1.ValueType_VALUE_TYPE_ARRAY {
			return fmt.Errorf("field %d ARRAY requires type_descriptor", fieldIndex)
		}
		descriptors[fieldIndex] = &kublingv1.TypeDescriptor{Type: valueType}
	}

	return validateBatchTuples(batch, descriptors)
}

func validateBatchTuples(
	batch *providerv1.TupleBatch,
	descriptors []*kublingv1.TypeDescriptor,
) error {
	for tupleIndex, tuple := range batch.GetTuples() {
		if tuple == nil {
			return fmt.Errorf("tuple %d is nil", tupleIndex)
		}
		if len(tuple.GetValues()) != len(descriptors) {
			return fmt.Errorf(
				"tuple %d contains %d values for %d fields",
				tupleIndex,
				len(tuple.GetValues()),
				len(descriptors),
			)
		}
		for valueIndex, value := range tuple.GetValues() {
			if err := validateValueAgainstDescriptor(
				value,
				descriptors[valueIndex],
			); err != nil {
				return fmt.Errorf(
					"tuple %d value %d: %w",
					tupleIndex,
					valueIndex,
					err,
				)
			}
		}
	}

	return nil
}

func validateValueAgainstDescriptor(
	value *kublingv1.Value,
	descriptor *kublingv1.TypeDescriptor,
) error {
	if descriptor != nil {
		if err := validateTypeDescriptor(descriptor); err != nil {
			return fmt.Errorf("declared type: %w", err)
		}
	}

	return validateValuePath(
		value,
		descriptor,
		make(map[*kublingv1.Value]struct{}),
	)
}

func validateValuePath(
	value *kublingv1.Value,
	descriptor *kublingv1.TypeDescriptor,
	path map[*kublingv1.Value]struct{},
) error {
	if value == nil {
		return fmt.Errorf("value is nil; use null_value for SQL NULL")
	}
	if _, cyclic := path[value]; cyclic {
		return fmt.Errorf("value contains an array cycle")
	}
	path[value] = struct{}{}
	defer delete(path, value)

	var actualType kublingv1.ValueType
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_NullValue:
		if typed.NullValue == nil {
			return fmt.Errorf("null_value is nil")
		}
		return nil
	case *kublingv1.Value_StringValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_STRING
	case *kublingv1.Value_VarbinaryValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_VARBINARY
	case *kublingv1.Value_CharValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_CHAR
	case *kublingv1.Value_BooleanValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_BOOLEAN
	case *kublingv1.Value_ByteValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_BYTE
	case *kublingv1.Value_ShortValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_SHORT
	case *kublingv1.Value_IntegerValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_INTEGER
	case *kublingv1.Value_LongValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_LONG
	case *kublingv1.Value_BigintegerValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_BIGINTEGER
	case *kublingv1.Value_FloatValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_FLOAT
	case *kublingv1.Value_DoubleValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_DOUBLE
	case *kublingv1.Value_BigdecimalValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL
	case *kublingv1.Value_DateValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_DATE
	case *kublingv1.Value_TimeValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_TIME
	case *kublingv1.Value_TimestampValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_TIMESTAMP
	case *kublingv1.Value_BlobValue:
		if typed.BlobValue == nil {
			return fmt.Errorf("blob_value is nil")
		}
		actualType = kublingv1.ValueType_VALUE_TYPE_BLOB
	case *kublingv1.Value_ClobValue:
		if typed.ClobValue == nil {
			return fmt.Errorf("clob_value is nil")
		}
		actualType = kublingv1.ValueType_VALUE_TYPE_CLOB
	case *kublingv1.Value_XmlValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_XML
	case *kublingv1.Value_GeometryValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_GEOMETRY
	case *kublingv1.Value_GeographyValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_GEOGRAPHY
	case *kublingv1.Value_JsonValue:
		actualType = kublingv1.ValueType_VALUE_TYPE_JSON
	case *kublingv1.Value_ArrayValue:
		if typed.ArrayValue == nil {
			return fmt.Errorf("array_value is nil")
		}
		actualType = kublingv1.ValueType_VALUE_TYPE_ARRAY
	case *kublingv1.Value_GeometryWithCrs:
		if typed.GeometryWithCrs == nil {
			return fmt.Errorf("geometry_with_crs is nil")
		}
		actualType = kublingv1.ValueType_VALUE_TYPE_GEOMETRY
	case *kublingv1.Value_GeographyWithCrs:
		if typed.GeographyWithCrs == nil {
			return fmt.Errorf("geography_with_crs is nil")
		}
		actualType = kublingv1.ValueType_VALUE_TYPE_GEOGRAPHY
	case *kublingv1.Value_LobReference:
		if typed.LobReference == nil {
			return fmt.Errorf("lob_reference is nil")
		}
		actualType = typed.LobReference.GetType()
		if actualType != kublingv1.ValueType_VALUE_TYPE_BLOB &&
			actualType != kublingv1.ValueType_VALUE_TYPE_CLOB {
			return fmt.Errorf("lob_reference type must be BLOB or CLOB")
		}
	default:
		return fmt.Errorf("value kind is required")
	}

	if descriptor != nil && descriptor.GetType() != actualType {
		return fmt.Errorf(
			"value type %s does not match declared type %s",
			actualType,
			descriptor.GetType(),
		)
	}

	arrayKind, isArray := value.GetKind().(*kublingv1.Value_ArrayValue)
	if !isArray {
		return nil
	}

	elementType := arrayKind.ArrayValue.GetElementType()
	if err := validateTypeDescriptor(elementType); err != nil {
		return fmt.Errorf("array element_type: %w", err)
	}
	if descriptor != nil &&
		!proto.Equal(descriptor.GetElementType(), elementType) {
		return fmt.Errorf(
			"array element_type does not match declared element_type",
		)
	}
	for elementIndex, element := range arrayKind.ArrayValue.GetElements() {
		if err := validateValuePath(element, elementType, path); err != nil {
			return fmt.Errorf("array element %d: %w", elementIndex, err)
		}
	}

	return nil
}

func validateExpressionValues(expression *providerv1.Expression) error {
	if expression == nil {
		return nil
	}

	switch typed := expression.GetKind().(type) {
	case *providerv1.Expression_Literal:
		if typed.Literal == nil {
			return fmt.Errorf("literal is nil")
		}
		return validateValueAgainstDescriptor(
			typed.Literal.GetValue(),
			typed.Literal.GetDeclaredType(),
		)
	case *providerv1.Expression_Comparison:
		if typed.Comparison == nil {
			return nil
		}
		if err := validateExpressionValues(typed.Comparison.GetLeft()); err != nil {
			return fmt.Errorf("comparison left: %w", err)
		}
		if err := validateExpressionValues(typed.Comparison.GetRight()); err != nil {
			return fmt.Errorf("comparison right: %w", err)
		}
	case *providerv1.Expression_Logical:
		if typed.Logical == nil {
			return nil
		}
		for operandIndex, operand := range typed.Logical.GetOperands() {
			if err := validateExpressionValues(operand); err != nil {
				return fmt.Errorf("logical operand %d: %w", operandIndex, err)
			}
		}
	case *providerv1.Expression_NullPredicate:
		if typed.NullPredicate != nil {
			return validateExpressionValues(typed.NullPredicate.GetExpression())
		}
	case *providerv1.Expression_FunctionCall:
		if typed.FunctionCall == nil {
			return nil
		}
		for argumentIndex, argument := range typed.FunctionCall.GetArguments() {
			if err := validateExpressionValues(argument); err != nil {
				return fmt.Errorf("function argument %d: %w", argumentIndex, err)
			}
		}
	case *providerv1.Expression_Pattern:
		if typed.Pattern == nil {
			return nil
		}
		if err := validateExpressionValues(typed.Pattern.GetValue()); err != nil {
			return fmt.Errorf("pattern value: %w", err)
		}
		if err := validateExpressionValues(typed.Pattern.GetPattern()); err != nil {
			return fmt.Errorf("pattern expression: %w", err)
		}
	}

	return nil
}

func validateQueryRequestValues(request *providerv1.QueryRequest) error {
	for projectionIndex, projection := range request.GetProjections() {
		if projection == nil {
			continue
		}
		if err := validateExpressionValues(projection.GetExpression()); err != nil {
			return fmt.Errorf("projection %d: %w", projectionIndex, err)
		}
	}
	if err := validateExpressionValues(request.GetFilter()); err != nil {
		return fmt.Errorf("filter: %w", err)
	}
	for orderIndex, order := range request.GetOrderBy() {
		if order == nil {
			continue
		}
		if err := validateExpressionValues(order.GetExpression()); err != nil {
			return fmt.Errorf("order_by %d: %w", orderIndex, err)
		}
	}

	return nil
}
