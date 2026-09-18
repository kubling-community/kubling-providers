package provider

import (
	"strings"
	"testing"

	grpcfeatures "github.com/kubling-community/kubling-grpc/sdk-go/features"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestValidateProviderCapabilitiesAcceptsLegacyAndFutureFeatures(t *testing.T) {
	tests := []*Capabilities{
		{},
		{Values: &providerv1.ValueCapabilities{}},
		{Values: &providerv1.ValueCapabilities{
			Features: []string{"future_value_feature_v1"},
		}},
	}

	for index, capabilities := range tests {
		if err := validateProviderCapabilities(capabilities); err != nil {
			t.Fatalf("case %d: validateProviderCapabilities() error = %v", index, err)
		}
	}
}

func TestValidateProviderCapabilitiesAcceptsConsistentExtendedValues(t *testing.T) {
	maxArrayDimensions := uint32(2)
	maxLobChunkBytes := uint32(64 * 1024)
	maxLobBytes := uint64(1024 * 1024)
	lobRetentionSeconds := uint64(300)
	capabilities := &Capabilities{Values: &providerv1.ValueCapabilities{
		SupportedTypes: []*providerv1.SupportedValueType{
			{Type: kublingv1.ValueType_VALUE_TYPE_ARRAY, Input: true, Output: true},
			{Type: kublingv1.ValueType_VALUE_TYPE_GEOMETRY, Output: true},
			{Type: kublingv1.ValueType_VALUE_TYPE_BLOB, Output: true},
		},
		Features: []string{
			grpcfeatures.ArrayValuesV1,
			grpcfeatures.SpatialValuesV1,
			grpcfeatures.LobReadV1,
		},
		MaxArrayDimensions:           &maxArrayDimensions,
		MaxLobChunkBytes:             &maxLobChunkBytes,
		MaxLobBytes:                  &maxLobBytes,
		LobReferenceRetentionSeconds: &lobRetentionSeconds,
	}}

	if err := validateProviderCapabilities(capabilities); err != nil {
		t.Fatalf("validateProviderCapabilities() error = %v", err)
	}
}

func TestValidateProviderCapabilitiesRejectsContradictions(t *testing.T) {
	positive32 := uint32(1)
	zero32 := uint32(0)
	positive64 := uint64(1)
	zero64 := uint64(0)

	tests := []struct {
		name     string
		values   *providerv1.ValueCapabilities
		wantText string
	}{
		{
			name: "nil supported type",
			values: &providerv1.ValueCapabilities{
				SupportedTypes: []*providerv1.SupportedValueType{nil},
			},
			wantText: "supported_types[0] is nil",
		},
		{
			name: "unknown supported type",
			values: &providerv1.ValueCapabilities{SupportedTypes: []*providerv1.SupportedValueType{{
				Type: 99, Input: true,
			}}},
			wantText: "type 99 is unknown",
		},
		{
			name: "supported type without direction",
			values: &providerv1.ValueCapabilities{SupportedTypes: []*providerv1.SupportedValueType{{
				Type: kublingv1.ValueType_VALUE_TYPE_STRING,
			}}},
			wantText: "neither input nor output",
		},
		{
			name: "duplicate supported type",
			values: &providerv1.ValueCapabilities{SupportedTypes: []*providerv1.SupportedValueType{
				{Type: kublingv1.ValueType_VALUE_TYPE_STRING, Input: true},
				{Type: kublingv1.ValueType_VALUE_TYPE_STRING, Output: true},
			}},
			wantText: "duplicate VALUE_TYPE_STRING",
		},
		{
			name:     "noncanonical feature",
			values:   &providerv1.ValueCapabilities{Features: []string{" array_values_v1"}},
			wantText: "canonical feature name",
		},
		{
			name: "duplicate feature",
			values: &providerv1.ValueCapabilities{Features: []string{
				grpcfeatures.ArrayValuesV1,
				grpcfeatures.ArrayValuesV1,
			}},
			wantText: "duplicate \"array_values_v1\"",
		},
		{
			name: "array missing limit",
			values: &providerv1.ValueCapabilities{
				Features: []string{grpcfeatures.ArrayValuesV1},
			},
			wantText: "max_array_dimensions must be positive",
		},
		{
			name: "array zero limit",
			values: &providerv1.ValueCapabilities{
				SupportedTypes: []*providerv1.SupportedValueType{{
					Type: kublingv1.ValueType_VALUE_TYPE_ARRAY, Output: true,
				}},
				Features:           []string{grpcfeatures.ArrayValuesV1},
				MaxArrayDimensions: &zero32,
			},
			wantText: "max_array_dimensions must be positive",
		},
		{
			name: "array missing type support",
			values: &providerv1.ValueCapabilities{
				Features:           []string{grpcfeatures.ArrayValuesV1},
				MaxArrayDimensions: &positive32,
			},
			wantText: "must include ARRAY",
		},
		{
			name: "spatial missing type support",
			values: &providerv1.ValueCapabilities{
				Features: []string{grpcfeatures.SpatialValuesV1},
			},
			wantText: "must include GEOMETRY or GEOGRAPHY",
		},
		{
			name: "LOB missing chunk limit",
			values: &providerv1.ValueCapabilities{
				Features: []string{grpcfeatures.LobReadV1},
			},
			wantText: "max_lob_chunk_bytes must be positive",
		},
		{
			name: "LOB missing size limit",
			values: &providerv1.ValueCapabilities{
				Features:         []string{grpcfeatures.LobReadV1},
				MaxLobChunkBytes: &positive32,
			},
			wantText: "max_lob_bytes must be positive",
		},
		{
			name: "LOB zero size limit",
			values: &providerv1.ValueCapabilities{
				Features:         []string{grpcfeatures.LobReadV1},
				MaxLobChunkBytes: &positive32,
				MaxLobBytes:      &zero64,
			},
			wantText: "max_lob_bytes must be positive",
		},
		{
			name: "LOB missing retention",
			values: &providerv1.ValueCapabilities{
				Features:         []string{grpcfeatures.LobReadV1},
				MaxLobChunkBytes: &positive32,
				MaxLobBytes:      &positive64,
			},
			wantText: "lob_reference_retention_seconds must be positive",
		},
		{
			name: "LOB zero retention",
			values: &providerv1.ValueCapabilities{
				Features:                     []string{grpcfeatures.LobReadV1},
				MaxLobChunkBytes:             &positive32,
				MaxLobBytes:                  &positive64,
				LobReferenceRetentionSeconds: &zero64,
			},
			wantText: "lob_reference_retention_seconds must be positive",
		},
		{
			name: "LOB missing output type support",
			values: &providerv1.ValueCapabilities{
				SupportedTypes: []*providerv1.SupportedValueType{{
					Type: kublingv1.ValueType_VALUE_TYPE_BLOB, Input: true,
				}},
				Features:                     []string{grpcfeatures.LobReadV1},
				MaxLobChunkBytes:             &positive32,
				MaxLobBytes:                  &positive64,
				LobReferenceRetentionSeconds: &positive64,
			},
			wantText: "BLOB or CLOB output",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateProviderCapabilities(&Capabilities{Values: test.values})
			if err == nil || !strings.Contains(err.Error(), test.wantText) {
				t.Fatalf(
					"validateProviderCapabilities() error = %v, want text %q",
					err,
					test.wantText,
				)
			}
		})
	}
}
