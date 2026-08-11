package cassandra

import (
	"math/big"
	"reflect"
	"testing"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
)

func TestProviderToNativeCoversCoreCassandraTypes(t *testing.T) {
	timestamp, err := providerToNative(
		&kublingv1.Value{Kind: &kublingv1.Value_TimestampValue{TimestampValue: "2026-08-03T14:30:15.125Z"}},
		gocql.NewNativeType(4, gocql.TypeTimestamp, ""),
	)
	if err != nil {
		t.Fatalf("timestamp conversion error = %v", err)
	}
	if got := timestamp.(time.Time); !got.Equal(time.Date(2026, 8, 3, 14, 30, 15, 125000000, time.UTC)) {
		t.Fatalf("timestamp conversion = %v", got)
	}

	varint, err := providerToNative(
		&kublingv1.Value{Kind: &kublingv1.Value_BigintegerValue{BigintegerValue: "123456789012345678901234567890"}},
		gocql.NewNativeType(4, gocql.TypeVarint, ""),
	)
	if err != nil {
		t.Fatalf("varint conversion error = %v", err)
	}
	wantVarint, _ := new(big.Int).SetString("123456789012345678901234567890", 10)
	if varint.(*big.Int).Cmp(wantVarint) != 0 {
		t.Fatalf("varint conversion = %v, want %v", varint, wantVarint)
	}

	collectionType := gocql.NewNativeType(4, gocql.TypeCustom, "map<text,text>")
	collection, err := providerToNative(
		&kublingv1.Value{Kind: &kublingv1.Value_JsonValue{JsonValue: `{"owner":"platform"}`}},
		collectionType,
	)
	if err != nil {
		t.Fatalf("map conversion error = %v", err)
	}
	if !reflect.DeepEqual(collection, map[string]string{"owner": "platform"}) {
		t.Fatalf("map conversion = %#v", collection)
	}
}

func TestNativeToValuePreservesNullAndCollectionJSON(t *testing.T) {
	nullValue, err := nativeToValue(gocql.NewNativeType(4, gocql.TypeText, ""), nil)
	if err != nil {
		t.Fatalf("null conversion error = %v", err)
	}
	if nullValue.GetNullValue() == nil {
		t.Fatalf("null conversion = %v", nullValue)
	}

	collectionType := gocql.NewNativeType(4, gocql.TypeCustom, "map<text,text>")
	var nilCollection map[string]string
	nullCollection, err := nativeToValue(collectionType, nilCollection)
	if err != nil {
		t.Fatalf("null map conversion error = %v", err)
	}
	if nullCollection.GetNullValue() == nil {
		t.Fatalf("null map conversion = %v", nullCollection)
	}

	jsonValue, err := nativeToValue(collectionType, map[string]string{"sample": "true", "engine": "kubling"})
	if err != nil {
		t.Fatalf("map conversion error = %v", err)
	}
	if got := jsonValue.GetJsonValue(); got != `{"engine":"kubling","sample":"true"}` {
		t.Fatalf("map JSON = %q", got)
	}
}

func TestIntegerConversionRejectsOverflow(t *testing.T) {
	_, err := providerToNative(
		integerProviderValue(128),
		gocql.NewNativeType(4, gocql.TypeTinyInt, ""),
	)
	if err == nil {
		t.Fatal("tinyint overflow error = nil")
	}
}

func TestCassandraDurationRoundTrip(t *testing.T) {
	typeInfo := gocql.NewNativeType(4, gocql.TypeDuration, "")
	original := gocql.Duration{Months: 2, Days: 3, Nanoseconds: 4}
	value, err := nativeToValue(typeInfo, original)
	if err != nil {
		t.Fatalf("nativeToValue() error = %v", err)
	}
	converted, err := providerToNative(value, typeInfo)
	if err != nil {
		t.Fatalf("providerToNative() error = %v", err)
	}
	if !reflect.DeepEqual(converted, original) {
		t.Fatalf("duration round trip = %#v, want %#v", converted, original)
	}
}
