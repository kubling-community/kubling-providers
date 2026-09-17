package provider

import (
	"fmt"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/protobuf/proto"
)

func validateOutputBatchFeatures(
	batch *providerv1.TupleBatch,
	acceptedFeatures []string,
	lobTransportAvailable bool,
) error {
	if err := validateOutputBatchStructure(batch); err != nil {
		return err
	}

	accepted := make(map[string]struct{}, len(acceptedFeatures))
	for _, feature := range acceptedFeatures {
		accepted[feature] = struct{}{}
	}

	for fieldIndex, field := range batch.GetFields() {
		if field.GetType() == kublingv1.ValueType_VALUE_TYPE_ARRAY ||
			field.GetTypeDescriptor().GetType() == kublingv1.ValueType_VALUE_TYPE_ARRAY {
			if err := requireAcceptedFeature(accepted, grpcfeatures.ArrayValuesV1); err != nil {
				return fmt.Errorf("field %d: %w", fieldIndex, err)
			}
		}
	}

	for tupleIndex, tuple := range batch.GetTuples() {
		for valueIndex, value := range tuple.GetValues() {
			if err := validateOutputValueFeatures(
				value,
				accepted,
				lobTransportAvailable,
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

func validateOutputValueFeatures(
	value *kublingv1.Value,
	accepted map[string]struct{},
	lobTransportAvailable bool,
) error {
	if value == nil {
		return nil
	}

	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_ArrayValue:
		if err := requireAcceptedFeature(accepted, grpcfeatures.ArrayValuesV1); err != nil {
			return err
		}
		for elementIndex, element := range typed.ArrayValue.GetElements() {
			if err := validateOutputValueFeatures(
				element,
				accepted,
				lobTransportAvailable,
			); err != nil {
				return fmt.Errorf("array element %d: %w", elementIndex, err)
			}
		}
	case *kublingv1.Value_GeometryWithCrs,
		*kublingv1.Value_GeographyWithCrs:
		return requireAcceptedFeature(accepted, grpcfeatures.SpatialValuesV1)
	case *kublingv1.Value_LobReference:
		if err := requireAcceptedFeature(accepted, grpcfeatures.LobReadV1); err != nil {
			return err
		}
		if !lobTransportAvailable {
			return fmt.Errorf(
				"feature %q requires provider LOB transport",
				grpcfeatures.LobReadV1,
			)
		}
		reference := typed.LobReference
		if reference == nil || reference.GetLobId() == "" {
			return fmt.Errorf("provider LOB reference requires lob_id")
		}
		if reference.GetType() != kublingv1.ValueType_VALUE_TYPE_BLOB &&
			reference.GetType() != kublingv1.ValueType_VALUE_TYPE_CLOB {
			return fmt.Errorf("provider LOB reference type must be BLOB or CLOB")
		}
		if reference.ExpiresAtUnixMs == nil || reference.GetExpiresAtUnixMs() <= 0 {
			return fmt.Errorf("provider LOB reference requires a positive expiry")
		}
		if reference.GetSessionId() != "" {
			return fmt.Errorf("provider LOB reference must not set session_id")
		}
	}

	return nil
}

func prepareOutputBatch(
	batch *providerv1.TupleBatch,
	connectionID string,
) *providerv1.TupleBatch {
	if !batchContainsLobReference(batch) {
		return batch
	}

	prepared := proto.Clone(batch).(*providerv1.TupleBatch)
	for _, tuple := range prepared.GetTuples() {
		for _, value := range tuple.GetValues() {
			setLobReferenceConnection(value, connectionID)
		}
	}

	return prepared
}

func batchContainsLobReference(batch *providerv1.TupleBatch) bool {
	for _, tuple := range batch.GetTuples() {
		for _, value := range tuple.GetValues() {
			if valueContainsLobReference(value) {
				return true
			}
		}
	}

	return false
}

func valueContainsLobReference(value *kublingv1.Value) bool {
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_LobReference:
		return true
	case *kublingv1.Value_ArrayValue:
		for _, element := range typed.ArrayValue.GetElements() {
			if valueContainsLobReference(element) {
				return true
			}
		}
	}

	return false
}

func setLobReferenceConnection(
	value *kublingv1.Value,
	connectionID string,
) {
	switch typed := value.GetKind().(type) {
	case *kublingv1.Value_LobReference:
		typed.LobReference.SessionId = connectionID
	case *kublingv1.Value_ArrayValue:
		for _, element := range typed.ArrayValue.GetElements() {
			setLobReferenceConnection(element, connectionID)
		}
	}
}

func requireAcceptedFeature(
	accepted map[string]struct{},
	feature string,
) error {
	if _, ok := accepted[feature]; ok {
		return nil
	}

	return fmt.Errorf("feature %q was not accepted", feature)
}
