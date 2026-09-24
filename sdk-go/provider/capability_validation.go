package provider

import (
	"fmt"
	"strings"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

type valueTypeDirections struct {
	input  bool
	output bool
}

func validateProviderCapabilities(capabilities *Capabilities) error {
	if err := validateAggregateCapabilities(
		capabilities.GetQuery().GetAggregates(),
	); err != nil {
		return err
	}

	values := capabilities.GetValues()
	if values == nil {
		return nil
	}

	supportedTypes := make(
		map[kublingv1.ValueType]valueTypeDirections,
		len(values.GetSupportedTypes()),
	)
	for index, supportedType := range values.GetSupportedTypes() {
		if supportedType == nil {
			return fmt.Errorf("values.supported_types[%d] is nil", index)
		}

		valueType := supportedType.GetType()
		if valueType == kublingv1.ValueType_VALUE_TYPE_UNKNOWN {
			return fmt.Errorf(
				"values.supported_types[%d].type is VALUE_TYPE_UNKNOWN",
				index,
			)
		}
		if _, known := kublingv1.ValueType_name[int32(valueType)]; !known {
			return fmt.Errorf(
				"values.supported_types[%d].type %d is unknown",
				index,
				valueType,
			)
		}
		if !supportedType.GetInput() && !supportedType.GetOutput() {
			return fmt.Errorf(
				"values.supported_types[%d] %s has neither input nor output support",
				index,
				valueType,
			)
		}
		if _, duplicate := supportedTypes[valueType]; duplicate {
			return fmt.Errorf(
				"values.supported_types contains duplicate %s",
				valueType,
			)
		}
		supportedTypes[valueType] = valueTypeDirections{
			input:  supportedType.GetInput(),
			output: supportedType.GetOutput(),
		}
	}

	features := make(map[string]struct{}, len(values.GetFeatures()))
	for index, feature := range values.GetFeatures() {
		if feature == "" || strings.TrimSpace(feature) != feature {
			return fmt.Errorf(
				"values.features[%d] must be a non-empty canonical feature name",
				index,
			)
		}
		if _, duplicate := features[feature]; duplicate {
			return fmt.Errorf("values.features contains duplicate %q", feature)
		}
		features[feature] = struct{}{}
	}

	if _, advertised := features[grpcfeatures.ArrayValuesV1]; advertised {
		if values.MaxArrayDimensions == nil || values.GetMaxArrayDimensions() == 0 {
			return fmt.Errorf(
				"values.max_array_dimensions must be positive when %s is advertised",
				grpcfeatures.ArrayValuesV1,
			)
		}
		if !supportsEitherDirection(
			supportedTypes,
			kublingv1.ValueType_VALUE_TYPE_ARRAY,
		) {
			return fmt.Errorf(
				"values.supported_types must include ARRAY when %s is advertised",
				grpcfeatures.ArrayValuesV1,
			)
		}
	}

	if _, advertised := features[grpcfeatures.SpatialValuesV1]; advertised &&
		!supportsEitherDirection(
			supportedTypes,
			kublingv1.ValueType_VALUE_TYPE_GEOMETRY,
		) &&
		!supportsEitherDirection(
			supportedTypes,
			kublingv1.ValueType_VALUE_TYPE_GEOGRAPHY,
		) {
		return fmt.Errorf(
			"values.supported_types must include GEOMETRY or GEOGRAPHY when %s is advertised",
			grpcfeatures.SpatialValuesV1,
		)
	}

	if _, advertised := features[grpcfeatures.LobReadV1]; advertised {
		if values.MaxLobChunkBytes == nil || values.GetMaxLobChunkBytes() == 0 {
			return fmt.Errorf(
				"values.max_lob_chunk_bytes must be positive when %s is advertised",
				grpcfeatures.LobReadV1,
			)
		}
		if values.MaxLobBytes == nil || values.GetMaxLobBytes() == 0 {
			return fmt.Errorf(
				"values.max_lob_bytes must be positive when %s is advertised",
				grpcfeatures.LobReadV1,
			)
		}
		if values.LobReferenceRetentionSeconds == nil ||
			values.GetLobReferenceRetentionSeconds() == 0 {
			return fmt.Errorf(
				"values.lob_reference_retention_seconds must be positive when %s is advertised",
				grpcfeatures.LobReadV1,
			)
		}
		if !supportsOutput(
			supportedTypes,
			kublingv1.ValueType_VALUE_TYPE_BLOB,
		) && !supportsOutput(
			supportedTypes,
			kublingv1.ValueType_VALUE_TYPE_CLOB,
		) {
			return fmt.Errorf(
				"values.supported_types must include BLOB or CLOB output when %s is advertised",
				grpcfeatures.LobReadV1,
			)
		}
	}

	return nil
}

func validateAggregateCapabilities(
	aggregates *providerv1.AggregateCapabilities,
) error {
	if aggregates == nil {
		return nil
	}

	functions := make(
		map[providerv1.AggregateFunction]struct{},
		len(aggregates.GetFunctions()),
	)
	for index, function := range aggregates.GetFunctions() {
		if function == providerv1.AggregateFunction_AGGREGATE_FUNCTION_UNSPECIFIED {
			return fmt.Errorf(
				"query.aggregates.functions[%d] is AGGREGATE_FUNCTION_UNSPECIFIED",
				index,
			)
		}
		if _, known := providerv1.AggregateFunction_name[int32(function)]; !known {
			return fmt.Errorf(
				"query.aggregates.functions[%d] %d is unknown",
				index,
				function,
			)
		}
		if _, duplicate := functions[function]; duplicate {
			return fmt.Errorf(
				"query.aggregates.functions contains duplicate %s",
				function,
			)
		}
		functions[function] = struct{}{}
	}

	return nil
}

func supportsEitherDirection(
	supportedTypes map[kublingv1.ValueType]valueTypeDirections,
	valueType kublingv1.ValueType,
) bool {
	directions, found := supportedTypes[valueType]
	return found && (directions.input || directions.output)
}

func supportsOutput(
	supportedTypes map[kublingv1.ValueType]valueTypeDirections,
	valueType kublingv1.ValueType,
) bool {
	directions, found := supportedTypes[valueType]
	return found && directions.output
}
