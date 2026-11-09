package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/catalog"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type objectMutation struct {
	key                string
	content            []byte
	contentType        string
	contentEncoding    string
	contentDisposition string
	cacheControl       string
	metadata           map[string]string
}

// Insert writes each tuple as one complete replacement object.
func (c *Connection) Insert(
	ctx context.Context,
	request *providerv1.InsertRequest,
) (*providerv1.InsertResponse, error) {
	if err := c.lockOpen(ctx); err != nil {
		return nil, err
	}
	defer c.unlockOpen()
	if request == nil || request.GetRows() == nil {
		return nil, status.Error(codes.InvalidArgument, "insert rows are required")
	}
	if len(request.GetReturningFields()) != 0 {
		return nil, status.Error(codes.Unimplemented, "generated INSERT values are not supported")
	}
	route, store, err := c.route(request.GetEntity())
	if err != nil {
		return nil, err
	}
	fields, err := mutationFields(request.GetRows().GetFields())
	if err != nil {
		return nil, err
	}
	var affected uint64
	for rowIndex, tuple := range request.GetRows().GetTuples() {
		mutation, err := decodeInsertTuple(route, fields, tuple)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "insert row %d: %v", rowIndex, err)
		}
		if uint64(len(mutation.content)) > c.lobPolicy.MaxBytes {
			return nil, status.Errorf(codes.ResourceExhausted, "insert row %d exceeds the object size limit", rowIndex)
		}
		size := int64(len(mutation.content))
		_, err = store.Put(ctx, route.Target, backend.PutRequest{
			Key:                mutation.key,
			Body:               bytes.NewReader(mutation.content),
			Size:               &size,
			ContentType:        mutation.contentType,
			ContentEncoding:    mutation.contentEncoding,
			ContentDisposition: mutation.contentDisposition,
			CacheControl:       mutation.cacheControl,
			Metadata:           mutation.metadata,
			IfNoneMatch:        true,
		})
		if err != nil {
			if errors.Is(err, backend.ErrConflict) {
				return nil, status.Errorf(codes.AlreadyExists, "insert row %d: object key already exists", rowIndex)
			}
			return nil, backendStatus("insert object", err)
		}
		affected++
	}
	return &providerv1.InsertResponse{AffectedRows: &affected}, nil
}

// Update changes one key-bound object. Assigning content performs a complete
// replacement; attribute-only assignments preserve the object body.
func (c *Connection) Update(
	ctx context.Context,
	request *providerv1.UpdateRequest,
) (*providerv1.UpdateResponse, error) {
	if err := c.lockOpen(ctx); err != nil {
		return nil, err
	}
	defer c.unlockOpen()
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "update request is required")
	}
	route, store, err := c.route(request.GetEntity())
	if err != nil {
		return nil, err
	}
	identity, err := parseIdentityFilter(request.GetFilter(), route.Namespace)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan update filter: %v", err)
	}
	if identity.noMatch {
		zero := uint64(0)
		return &providerv1.UpdateResponse{AffectedRows: &zero}, nil
	}
	if !identity.hasKey {
		return nil, status.Error(codes.FailedPrecondition, "UPDATE requires key equality")
	}
	current, err := store.Stat(ctx, route.Target, identity.key)
	if errors.Is(err, backend.ErrNotFound) {
		zero := uint64(0)
		return &providerv1.UpdateResponse{AffectedRows: &zero}, nil
	}
	if err != nil {
		return nil, backendStatus("stat object for update", err)
	}
	if current.Revision == "" {
		return nil, status.Error(
			codes.FailedPrecondition,
			"object backend did not provide a revision for a conditional update",
		)
	}
	mutation := objectMutation{
		key:                identity.key,
		contentType:        current.ContentType,
		contentEncoding:    current.ContentEncoding,
		contentDisposition: current.ContentDisposition,
		cacheControl:       current.CacheControl,
		metadata:           cloneStringMap(current.Metadata),
	}
	assigned, err := applyAssignments(&mutation, request.GetAssignments())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan object update: %v", err)
	}
	if assigned.content {
		if uint64(len(mutation.content)) > c.lobPolicy.MaxBytes {
			return nil, status.Error(codes.ResourceExhausted, "updated object exceeds the object size limit")
		}
		size := int64(len(mutation.content))
		_, err = store.Put(ctx, route.Target, backend.PutRequest{
			Key:                mutation.key,
			Body:               bytes.NewReader(mutation.content),
			Size:               &size,
			ContentType:        mutation.contentType,
			ContentEncoding:    mutation.contentEncoding,
			ContentDisposition: mutation.contentDisposition,
			CacheControl:       mutation.cacheControl,
			Metadata:           mutation.metadata,
			IfRevisionMatch:    current.Revision,
		})
	} else {
		err = store.UpdateAttributes(ctx, route.Target, backend.UpdateAttributesRequest{
			Key:                mutation.key,
			Size:               current.Size,
			ContentType:        mutation.contentType,
			ContentEncoding:    mutation.contentEncoding,
			ContentDisposition: mutation.contentDisposition,
			CacheControl:       mutation.cacheControl,
			Metadata:           mutation.metadata,
			Fields:             assigned.attributes,
			IfRevisionMatch:    current.Revision,
		})
	}
	if err != nil {
		return nil, backendStatus("update object", err)
	}
	one := uint64(1)
	return &providerv1.UpdateResponse{AffectedRows: &one}, nil
}

// Delete removes exactly one key-bound object.
func (c *Connection) Delete(
	ctx context.Context,
	request *providerv1.DeleteRequest,
) (*providerv1.DeleteResponse, error) {
	if err := c.lockOpen(ctx); err != nil {
		return nil, err
	}
	defer c.unlockOpen()
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "delete request is required")
	}
	route, store, err := c.route(request.GetEntity())
	if err != nil {
		return nil, err
	}
	identity, err := parseIdentityFilter(request.GetFilter(), route.Namespace)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "plan delete filter: %v", err)
	}
	if identity.noMatch {
		zero := uint64(0)
		return &providerv1.DeleteResponse{AffectedRows: &zero}, nil
	}
	if !identity.hasKey {
		return nil, status.Error(codes.FailedPrecondition, "DELETE requires key equality")
	}
	current, err := store.Stat(ctx, route.Target, identity.key)
	if errors.Is(err, backend.ErrNotFound) {
		zero := uint64(0)
		return &providerv1.DeleteResponse{AffectedRows: &zero}, nil
	}
	if err != nil {
		return nil, backendStatus("stat object for delete", err)
	}
	if current.Revision == "" {
		return nil, status.Error(
			codes.FailedPrecondition,
			"object backend did not provide a revision for a conditional delete",
		)
	}
	if err := store.Delete(ctx, route.Target, backend.DeleteRequest{
		Key:             identity.key,
		IfRevisionMatch: current.Revision,
	}); err != nil {
		return nil, backendStatus("delete object", err)
	}
	one := uint64(1)
	return &providerv1.DeleteResponse{AffectedRows: &one}, nil
}

func mutationFields(fields []*providerv1.Field) ([]string, error) {
	result := make([]string, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for index, field := range fields {
		if field == nil || field.GetName() == "" {
			return nil, fmt.Errorf("field %d is unnamed", index)
		}
		name := normalizeName(field.GetName())
		if !mutableColumn(name) && name != catalog.BucketColumn && name != catalog.KeyColumn {
			return nil, fmt.Errorf("field %q is not insertable", field.GetName())
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("field %q is duplicated", field.GetName())
		}
		seen[name] = struct{}{}
		result[index] = name
	}
	return result, nil
}

func decodeInsertTuple(
	route catalog.Bucket,
	fields []string,
	tuple *providerv1.Tuple,
) (objectMutation, error) {
	if tuple == nil || len(tuple.GetValues()) != len(fields) {
		return objectMutation{}, fmt.Errorf("tuple value count does not match its fields")
	}
	var mutation objectMutation
	keySet := false
	contentSet := false
	for index, field := range fields {
		value := tuple.GetValues()[index]
		var err error
		switch field {
		case catalog.BucketColumn:
			var bucket string
			bucket, err = requiredString(value)
			if err == nil && bucket != route.Namespace {
				err = fmt.Errorf("bucket must equal logical namespace %q", route.Namespace)
			}
		case catalog.KeyColumn:
			mutation.key, err = requiredString(value)
			keySet = err == nil
		case catalog.ContentColumn:
			mutation.content, err = requiredBlob(value)
			contentSet = err == nil
		case catalog.ContentTypeColumn:
			mutation.contentType, err = optionalStringValue(value)
		case catalog.ContentEncodingColumn:
			mutation.contentEncoding, err = optionalStringValue(value)
		case catalog.ContentDispositionColumn:
			mutation.contentDisposition, err = optionalStringValue(value)
		case catalog.CacheControlColumn:
			mutation.cacheControl, err = optionalStringValue(value)
		case catalog.MetadataColumn:
			mutation.metadata, err = metadataValue(value)
		}
		if err != nil {
			return objectMutation{}, fmt.Errorf("field %q: %w", field, err)
		}
	}
	if !keySet {
		return objectMutation{}, fmt.Errorf("key is required")
	}
	if err := backend.ValidateKey(mutation.key); err != nil {
		return objectMutation{}, err
	}
	if !contentSet {
		return objectMutation{}, fmt.Errorf("content is required")
	}
	return mutation, nil
}

type assignedMutationFields struct {
	content    bool
	attributes backend.AttributeFields
}

func applyAssignments(
	mutation *objectMutation,
	assignments []*providerv1.Assignment,
) (assignedMutationFields, error) {
	if len(assignments) == 0 {
		return assignedMutationFields{}, fmt.Errorf("at least one assignment is required")
	}
	seen := make(map[string]struct{}, len(assignments))
	var assigned assignedMutationFields
	for _, assignment := range assignments {
		if assignment == nil {
			return assignedMutationFields{}, fmt.Errorf("assignment is required")
		}
		field := normalizeName(assignment.GetField())
		if !mutableColumn(field) {
			return assignedMutationFields{}, fmt.Errorf("field %q is not updatable", assignment.GetField())
		}
		if _, duplicate := seen[field]; duplicate {
			return assignedMutationFields{}, fmt.Errorf("field %q is assigned more than once", assignment.GetField())
		}
		seen[field] = struct{}{}
		value, err := literalValue(assignment.GetValue())
		if err != nil {
			return assignedMutationFields{}, fmt.Errorf("field %q: %w", field, err)
		}
		switch field {
		case catalog.ContentColumn:
			mutation.content, err = requiredBlob(value)
			assigned.content = err == nil
		case catalog.ContentTypeColumn:
			mutation.contentType, err = optionalStringValue(value)
			assigned.attributes.ContentType = err == nil
		case catalog.ContentEncodingColumn:
			mutation.contentEncoding, err = optionalStringValue(value)
			assigned.attributes.ContentEncoding = err == nil
		case catalog.ContentDispositionColumn:
			mutation.contentDisposition, err = optionalStringValue(value)
			assigned.attributes.ContentDisposition = err == nil
		case catalog.CacheControlColumn:
			mutation.cacheControl, err = optionalStringValue(value)
			assigned.attributes.CacheControl = err == nil
		case catalog.MetadataColumn:
			mutation.metadata, err = metadataValue(value)
			assigned.attributes.Metadata = err == nil
		}
		if err != nil {
			return assignedMutationFields{}, fmt.Errorf("field %q: %w", field, err)
		}
	}
	return assigned, nil
}

func mutableColumn(name string) bool {
	switch name {
	case catalog.ContentColumn,
		catalog.ContentTypeColumn,
		catalog.ContentEncodingColumn,
		catalog.ContentDispositionColumn,
		catalog.CacheControlColumn,
		catalog.MetadataColumn:
		return true
	default:
		return false
	}
}

func requiredString(value *kublingv1.Value) (string, error) {
	if value == nil {
		return "", fmt.Errorf("value is required")
	}
	typed, ok := value.GetKind().(*kublingv1.Value_StringValue)
	if !ok {
		return "", fmt.Errorf("value must be STRING")
	}
	if typed.StringValue == "" {
		return "", fmt.Errorf("value must not be empty")
	}
	return typed.StringValue, nil
}

func optionalStringValue(value *kublingv1.Value) (string, error) {
	if value == nil {
		return "", fmt.Errorf("value is required")
	}
	if _, null := value.GetKind().(*kublingv1.Value_NullValue); null {
		return "", nil
	}
	typed, ok := value.GetKind().(*kublingv1.Value_StringValue)
	if !ok {
		return "", fmt.Errorf("value must be STRING or NULL")
	}
	return typed.StringValue, nil
}

func requiredBlob(value *kublingv1.Value) ([]byte, error) {
	if value == nil {
		return nil, fmt.Errorf("value is required")
	}
	typed, ok := value.GetKind().(*kublingv1.Value_BlobValue)
	if !ok || typed.BlobValue == nil {
		if value.GetLobReference() != nil {
			return nil, fmt.Errorf("provider-side LOB input streaming is not supported by this contract")
		}
		return nil, fmt.Errorf("value must be an inline BLOB")
	}
	return append([]byte(nil), typed.BlobValue.GetData()...), nil
}

func metadataValue(value *kublingv1.Value) (map[string]string, error) {
	if value == nil {
		return nil, fmt.Errorf("value is required")
	}
	if _, null := value.GetKind().(*kublingv1.Value_NullValue); null {
		return nil, nil
	}
	typed, ok := value.GetKind().(*kublingv1.Value_JsonValue)
	if !ok {
		return nil, fmt.Errorf("value must be JSON or NULL")
	}
	metadata := make(map[string]string)
	if err := json.Unmarshal([]byte(typed.JsonValue), &metadata); err != nil {
		return nil, fmt.Errorf("metadata must be a JSON object with string values: %w", err)
	}
	return metadata, nil
}

func cloneStringMap(value map[string]string) map[string]string {
	if len(value) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}
