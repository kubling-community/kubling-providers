package backend

import (
	"errors"
	"strings"
	"testing"
)

func TestPortableKeysRoundTripThroughPrefix(t *testing.T) {
	bucket := Bucket{Name: "physical", Prefix: "tenants/acme/"}
	physical, err := PhysicalKey(bucket, "images/logo.png")
	if err != nil {
		t.Fatalf("PhysicalKey() error = %v", err)
	}
	if physical != "tenants/acme/images/logo.png" {
		t.Fatalf("physical key = %q", physical)
	}
	logical, ok := LogicalKey(bucket, physical)
	if !ok || logical != "images/logo.png" {
		t.Fatalf("LogicalKey() = %q, %v", logical, ok)
	}
}

func TestValidateKeyRejectsUnsafeOrNonPortableValues(t *testing.T) {
	tests := []string{
		"",
		"/absolute",
		"directory//file",
		"directory/../file",
		"directory/./file",
		`directory\file`,
		"bad\x00key",
		strings.Repeat("a", MaxPortableKeyBytes+1),
		string([]byte{0xff}),
	}
	for _, key := range tests {
		var keyError *KeyError
		if err := ValidateKey(key); !errors.As(err, &keyError) {
			t.Errorf("ValidateKey(%q) error = %v", key, err)
		}
	}
}

func TestLogicalKeyRejectsAnotherConfiguredRoot(t *testing.T) {
	bucket := Bucket{Name: "physical", Prefix: "tenants/acme/"}
	if logical, ok := LogicalKey(bucket, "tenants/other/file"); ok {
		t.Fatalf("LogicalKey() = %q, true", logical)
	}
}
