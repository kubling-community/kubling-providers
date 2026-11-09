// Package backend defines the provider-neutral object storage boundary.
package backend

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

// ErrNotFound reports an object that is absent from the selected bucket.
var ErrNotFound = errors.New("object not found")

// ErrConflict reports a failed conditional object mutation.
var ErrConflict = errors.New("object mutation conflict")

const MaxPortableKeyBytes = 1024

// Bucket identifies one physical container and an optional provider-owned root.
type Bucket struct {
	Name   string
	Prefix string
}

// Object contains metadata shared by supported object-store implementations.
type Object struct {
	Key                string
	Size               int64
	ETag               string
	Revision           string
	LastModified       time.Time
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	CacheControl       string
	VersionID          string
	Checksum           string
	Metadata           map[string]string
}

// ObjectFields identifies object values that a listing guarantees are
// complete. An empty string or map is still a complete value when its field is
// present in this mask.
type ObjectFields uint32

const (
	ObjectFieldKey ObjectFields = 1 << iota
	ObjectFieldContent
	ObjectFieldContentType
	ObjectFieldContentEncoding
	ObjectFieldContentDisposition
	ObjectFieldCacheControl
	ObjectFieldContentLength
	ObjectFieldETag
	ObjectFieldLastModified
	ObjectFieldVersionID
	ObjectFieldChecksum
	ObjectFieldMetadata
)

// AllObjectFields contains every portable field exposed by an object listing.
const AllObjectFields = ObjectFieldKey |
	ObjectFieldContent |
	ObjectFieldContentType |
	ObjectFieldContentEncoding |
	ObjectFieldContentDisposition |
	ObjectFieldCacheControl |
	ObjectFieldContentLength |
	ObjectFieldETag |
	ObjectFieldLastModified |
	ObjectFieldVersionID |
	ObjectFieldChecksum |
	ObjectFieldMetadata

// Contains reports whether all required fields are complete.
func (f ObjectFields) Contains(required ObjectFields) bool {
	return f&required == required
}

// ListRequest describes one bounded object listing.
type ListRequest struct {
	Prefix            string
	StartAfter        string
	ContinuationToken string
	Limit             int32
}

// ListPage is one backend page. ContinuationToken is empty on the final page.
type ListPage struct {
	Objects           []Object
	ContinuationToken string
	Fields            ObjectFields
}

// GetRequest describes one complete or ranged object read.
type GetRequest struct {
	Key       string
	Offset    int64
	Length    *int64
	VersionID string
	Revision  string
}

// ObjectReader combines immutable object metadata with its streamed content.
type ObjectReader struct {
	Object Object
	Body   io.ReadCloser
}

// PutRequest streams one complete replacement object.
type PutRequest struct {
	Key                string
	Body               io.Reader
	Size               *int64
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	CacheControl       string
	Metadata           map[string]string
	IfRevisionMatch    string
	IfNoneMatch        bool
}

// AttributeFields identifies the portable attributes assigned by one update.
// Backends may use it to select the narrowest native mutation operation.
type AttributeFields struct {
	ContentType        bool
	ContentEncoding    bool
	ContentDisposition bool
	CacheControl       bool
	Metadata           bool
}

// Empty reports whether no portable attribute was assigned.
func (f AttributeFields) Empty() bool {
	return !f.ContentType &&
		!f.ContentEncoding &&
		!f.ContentDisposition &&
		!f.CacheControl &&
		!f.Metadata
}

// HTTPHeaders reports whether at least one portable HTTP header was assigned.
func (f AttributeFields) HTTPHeaders() bool {
	return f.ContentType || f.ContentEncoding || f.ContentDisposition || f.CacheControl
}

// UpdateAttributesRequest replaces selected portable attributes without
// transferring the object body. Values contain the complete desired state so a
// backend can use native replace-all metadata APIs without losing attributes
// that were not assigned by the SQL statement.
type UpdateAttributesRequest struct {
	Key                string
	Size               int64
	ContentType        string
	ContentEncoding    string
	ContentDisposition string
	CacheControl       string
	Metadata           map[string]string
	Fields             AttributeFields
	IfRevisionMatch    string
}

// DeleteRequest identifies one object version to remove.
type DeleteRequest struct {
	Key             string
	VersionID       string
	IfRevisionMatch string
}

// Store is the portable object-storage surface used by the provider.
//
// Implementations own native pagination and multipart upload details.
type Store interface {
	Probe(context.Context, Bucket) error
	List(context.Context, Bucket, ListRequest) (ListPage, error)
	Stat(context.Context, Bucket, string) (Object, error)
	Open(context.Context, Bucket, GetRequest) (ObjectReader, error)
	Put(context.Context, Bucket, PutRequest) (Object, error)
	UpdateAttributes(context.Context, Bucket, UpdateAttributesRequest) error
	Delete(context.Context, Bucket, DeleteRequest) error
	Close() error
}

// ValidateKey enforces the portable key subset shared by cloud and local
// object-store implementations.
func ValidateKey(key string) error {
	if key == "" {
		return &KeyError{Key: key, Reason: "key is empty"}
	}
	if !utf8.ValidString(key) {
		return &KeyError{Key: key, Reason: "key is not valid UTF-8"}
	}
	if len(key) > MaxPortableKeyBytes {
		return &KeyError{Key: key, Reason: "key exceeds the portable 1024-byte limit"}
	}
	if strings.HasPrefix(key, "/") ||
		strings.ContainsRune(key, '\x00') ||
		strings.Contains(key, "\\") {
		return &KeyError{Key: key, Reason: "key contains an unsafe character"}
	}
	for _, segment := range strings.Split(key, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return &KeyError{Key: key, Reason: "key contains an unsafe path segment"}
		}
	}
	return nil
}

// PhysicalKey scopes a validated logical key below a configured bucket prefix.
func PhysicalKey(bucket Bucket, key string) (string, error) {
	if err := ValidateKey(key); err != nil {
		return "", err
	}
	return bucket.Prefix + key, nil
}

// LogicalKey removes the configured prefix from a physical backend key.
func LogicalKey(bucket Bucket, key string) (string, bool) {
	logical, found := strings.CutPrefix(key, bucket.Prefix)
	if !found || ValidateKey(logical) != nil {
		return "", false
	}
	return logical, true
}

// KeyError reports a key that cannot be represented portably.
type KeyError struct {
	Key    string
	Reason string
}

func (e *KeyError) Error() string {
	return "invalid object key: " + e.Reason
}
