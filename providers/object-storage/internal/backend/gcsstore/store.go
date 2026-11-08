// Package gcsstore implements the portable backend using Google Cloud Storage.
package gcsstore

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	providerconfig "github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const defaultListPageSize = 1000

// Store is safe for concurrent use.
type Store struct {
	client          *storage.Client
	uploadChunkSize int
}

// New creates a GCS backend using Application Default Credentials or an
// explicitly configured unauthenticated emulator endpoint.
func New(ctx context.Context, configuration providerconfig.Backend) (*Store, error) {
	if configuration.Type != "gcs" {
		return nil, fmt.Errorf("GCS backend received type %q", configuration.Type)
	}

	options := []option.ClientOption{
		storage.WithXMLReads(),
		storage.WithDisabledClientMetrics(),
	}
	switch configuration.Authentication {
	case "default":
	case "emulator":
		options = append(
			options,
			option.WithEndpoint(emulatorServiceEndpoint(configuration.Endpoint)),
			option.WithoutAuthentication(),
		)
	default:
		return nil, fmt.Errorf("unsupported GCS authentication %q", configuration.Authentication)
	}

	client, err := storage.NewClient(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("create GCS client: %w", err)
	}
	return newWithClient(client, int(configuration.UploadChunkSizeBytes)), nil
}

func newWithClient(client *storage.Client, uploadChunkSize int) *Store {
	return &Store{client: client, uploadChunkSize: uploadChunkSize}
}

// Probe verifies access to one configured bucket without listing its objects.
func (s *Store) Probe(ctx context.Context, bucket backend.Bucket) error {
	_, err := s.client.Bucket(bucket.Name).Attrs(ctx)
	if err != nil {
		return fmt.Errorf("probe GCS bucket: %w", normalizeError(err))
	}
	return nil
}

// List returns one native GCS page below the configured root prefix.
func (s *Store) List(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.ListRequest,
) (backend.ListPage, error) {
	if request.Limit < 0 {
		return backend.ListPage{}, errors.New("GCS list limit must not be negative")
	}
	query := &storage.Query{
		Prefix:     bucket.Prefix + request.Prefix,
		Projection: storage.ProjectionNoACL,
	}
	if request.StartAfter != "" {
		startOffset, err := backend.PhysicalKey(bucket, request.StartAfter)
		if err != nil {
			return backend.ListPage{}, err
		}
		query.StartOffset = startOffset
	}

	pageSize := int(request.Limit)
	if pageSize == 0 {
		pageSize = defaultListPageSize
	}
	objectIterator := s.client.Bucket(bucket.Name).Objects(ctx, query)
	var attributes []*storage.ObjectAttrs
	nextToken, err := iterator.NewPager(
		objectIterator,
		pageSize,
		request.ContinuationToken,
	).NextPage(&attributes)
	if err != nil {
		return backend.ListPage{}, fmt.Errorf("list GCS objects: %w", normalizeError(err))
	}

	page := backend.ListPage{
		Objects:           make([]backend.Object, 0, len(attributes)),
		ContinuationToken: nextToken,
		Fields:            backend.AllObjectFields,
	}
	for _, attributes := range attributes {
		if attributes == nil {
			continue
		}
		logicalKey, ok := backend.LogicalKey(bucket, attributes.Name)
		if !ok || logicalKey == request.StartAfter {
			continue
		}
		page.Objects = append(page.Objects, objectFromAttrs(logicalKey, attributes))
	}
	return page, nil
}

// Stat reads object attributes without downloading content.
func (s *Store) Stat(
	ctx context.Context,
	bucket backend.Bucket,
	key string,
) (backend.Object, error) {
	handle, err := s.objectHandle(bucket, key, "", "")
	if err != nil {
		return backend.Object{}, err
	}
	attributes, err := handle.Attrs(ctx)
	if err != nil {
		err = normalizeError(err)
		if errors.Is(err, backend.ErrNotFound) {
			return backend.Object{}, err
		}
		return backend.Object{}, fmt.Errorf("stat GCS object: %w", err)
	}
	return objectFromAttrs(key, attributes), nil
}

// Open streams one complete object or byte range.
func (s *Store) Open(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.GetRequest,
) (backend.ObjectReader, error) {
	if request.Offset < 0 {
		return backend.ObjectReader{}, errors.New("GCS object offset must not be negative")
	}
	if request.Length != nil && *request.Length < 0 {
		return backend.ObjectReader{}, errors.New("GCS object length must not be negative")
	}
	handle, err := s.objectHandle(bucket, request.Key, request.VersionID, request.Revision)
	if err != nil {
		return backend.ObjectReader{}, err
	}
	handle = handle.ReadCompressed(true)
	if request.Length != nil && *request.Length == 0 {
		attributes, err := handle.Attrs(ctx)
		if err != nil {
			return backend.ObjectReader{}, normalizeError(err)
		}
		return backend.ObjectReader{
			Object: objectFromAttrs(request.Key, attributes),
			Body:   io.NopCloser(strings.NewReader("")),
		}, nil
	}

	length := int64(-1)
	if request.Length != nil {
		length = *request.Length
	}
	reader, err := handle.NewRangeReader(ctx, request.Offset, length)
	if err != nil {
		err = normalizeError(err)
		if errors.Is(err, backend.ErrNotFound) || errors.Is(err, backend.ErrConflict) {
			return backend.ObjectReader{}, err
		}
		return backend.ObjectReader{}, fmt.Errorf("open GCS object: %w", err)
	}
	return backend.ObjectReader{
		Object: objectFromReader(request.Key, reader.Attrs),
		Body:   reader,
	}, nil
}

// Put uploads one complete replacement object.
func (s *Store) Put(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.PutRequest,
) (backend.Object, error) {
	if request.Body == nil {
		return backend.Object{}, errors.New("GCS object body is required")
	}
	if request.Size != nil && *request.Size < 0 {
		return backend.Object{}, errors.New("GCS object size must not be negative")
	}
	if request.IfNoneMatch && request.IfRevisionMatch != "" {
		return backend.Object{}, errors.New("GCS put cannot combine revision and nonexistence conditions")
	}
	handle, err := s.objectHandle(bucket, request.Key, "", "")
	if err != nil {
		return backend.Object{}, err
	}
	if request.IfNoneMatch {
		handle = handle.If(storage.Conditions{DoesNotExist: true})
	} else if request.IfRevisionMatch != "" {
		conditions, err := parseRevision(request.IfRevisionMatch)
		if err != nil {
			return backend.Object{}, err
		}
		handle = handle.If(conditions)
	}

	writeContext, cancel := context.WithCancel(ctx)
	defer cancel()
	writer := handle.NewWriter(writeContext)
	if s.uploadChunkSize > 0 {
		writer.ChunkSize = s.uploadChunkSize
	}
	writer.ContentType = request.ContentType
	writer.ForceEmptyContentType = request.ContentType == ""
	writer.ContentEncoding = request.ContentEncoding
	writer.ContentDisposition = request.ContentDisposition
	writer.CacheControl = request.CacheControl
	writer.Metadata = cloneMetadata(request.Metadata)

	written, copyErr := io.Copy(writer, request.Body)
	if copyErr == nil && request.Size != nil && written != *request.Size {
		copyErr = fmt.Errorf(
			"GCS object size mismatch: expected %d bytes, received %d",
			*request.Size,
			written,
		)
	}
	if copyErr != nil {
		cancel()
		_ = writer.Close()
		return backend.Object{}, fmt.Errorf("upload GCS object: %w", copyErr)
	}
	if err := writer.Close(); err != nil {
		err = normalizeError(err)
		if errors.Is(err, backend.ErrConflict) {
			return backend.Object{}, err
		}
		return backend.Object{}, fmt.Errorf("upload GCS object: %w", err)
	}
	attributes := writer.Attrs()
	if attributes == nil {
		return backend.Object{}, errors.New("upload GCS object: backend returned no attributes")
	}
	return objectFromAttrs(request.Key, attributes), nil
}

// UpdateAttributes patches portable object attributes without rewriting data.
func (s *Store) UpdateAttributes(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.UpdateAttributesRequest,
) error {
	if request.Fields.Empty() {
		return errors.New("GCS attribute update requires at least one field")
	}
	if request.IfRevisionMatch == "" {
		return errors.New("GCS attribute update requires a revision")
	}
	handle, err := s.objectHandle(bucket, request.Key, "", request.IfRevisionMatch)
	if err != nil {
		return err
	}
	var update storage.ObjectAttrsToUpdate
	if request.Fields.ContentType {
		update.ContentType = request.ContentType
	}
	if request.Fields.ContentEncoding {
		update.ContentEncoding = request.ContentEncoding
	}
	if request.Fields.ContentDisposition {
		update.ContentDisposition = request.ContentDisposition
	}
	if request.Fields.CacheControl {
		update.CacheControl = request.CacheControl
	}
	if request.Fields.Metadata {
		update.Metadata = cloneMetadataForUpdate(request.Metadata)
	}
	if _, err := handle.Update(ctx, update); err != nil {
		err = normalizeError(err)
		if errors.Is(err, backend.ErrNotFound) || errors.Is(err, backend.ErrConflict) {
			return err
		}
		return fmt.Errorf("update GCS object attributes: %w", err)
	}
	return nil
}

// Delete removes one object generation under an optional generation condition.
func (s *Store) Delete(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.DeleteRequest,
) error {
	handle, err := s.objectHandle(
		bucket,
		request.Key,
		request.VersionID,
		request.IfRevisionMatch,
	)
	if err != nil {
		return err
	}
	if err := handle.Delete(ctx); err != nil {
		err = normalizeError(err)
		if errors.Is(err, backend.ErrNotFound) || errors.Is(err, backend.ErrConflict) {
			return err
		}
		return fmt.Errorf("delete GCS object: %w", err)
	}
	return nil
}

// Close releases the reusable GCS client transports.
func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) objectHandle(
	bucket backend.Bucket,
	key string,
	versionID string,
	revision string,
) (*storage.ObjectHandle, error) {
	physicalKey, err := backend.PhysicalKey(bucket, key)
	if err != nil {
		return nil, err
	}
	handle := s.client.Bucket(bucket.Name).Object(physicalKey)
	if versionID != "" {
		generation, err := parseGeneration("version", versionID)
		if err != nil {
			return nil, err
		}
		handle = handle.Generation(generation)
	}
	if revision != "" {
		conditions, err := parseRevision(revision)
		if err != nil {
			return nil, err
		}
		handle = handle.If(conditions)
	}
	return handle, nil
}

func objectFromAttrs(key string, attributes *storage.ObjectAttrs) backend.Object {
	if attributes == nil {
		return backend.Object{Key: key}
	}
	generation := generationString(attributes.Generation)
	return backend.Object{
		Key:                key,
		Size:               attributes.Size,
		ETag:               strings.Trim(attributes.Etag, "\""),
		Revision:           revisionString(attributes.Generation, attributes.Metageneration),
		LastModified:       attributes.Updated,
		ContentType:        attributes.ContentType,
		ContentEncoding:    attributes.ContentEncoding,
		ContentDisposition: attributes.ContentDisposition,
		CacheControl:       attributes.CacheControl,
		VersionID:          generation,
		Checksum:           objectChecksum(attributes),
		Metadata:           cloneMetadata(attributes.Metadata),
	}
}

func objectFromReader(key string, attributes storage.ReaderObjectAttrs) backend.Object {
	generation := generationString(attributes.Generation)
	return backend.Object{
		Key:             key,
		Size:            attributes.Size,
		Revision:        revisionString(attributes.Generation, attributes.Metageneration),
		LastModified:    attributes.LastModified,
		ContentType:     attributes.ContentType,
		ContentEncoding: attributes.ContentEncoding,
		CacheControl:    attributes.CacheControl,
		VersionID:       generation,
		Checksum:        readerChecksum(attributes.CRC32C),
	}
}

func parseGeneration(kind string, value string) (int64, error) {
	generation, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || generation <= 0 {
		return 0, fmt.Errorf("invalid GCS %s %q", kind, value)
	}
	return generation, nil
}

func parseRevision(value string) (storage.Conditions, error) {
	parts := strings.Split(strings.TrimSpace(value), ":")
	if len(parts) < 1 || len(parts) > 2 {
		return storage.Conditions{}, fmt.Errorf("invalid GCS revision %q", value)
	}
	generation, err := parseGeneration("revision", parts[0])
	if err != nil {
		return storage.Conditions{}, err
	}
	conditions := storage.Conditions{GenerationMatch: generation}
	if len(parts) == 2 {
		metageneration, err := parseGeneration("metadata revision", parts[1])
		if err != nil {
			return storage.Conditions{}, err
		}
		conditions.MetagenerationMatch = metageneration
	}
	return conditions, nil
}

func revisionString(generation int64, metageneration int64) string {
	value := generationString(generation)
	if value == "" || metageneration <= 0 {
		return value
	}
	return value + ":" + strconv.FormatInt(metageneration, 10)
}

func generationString(generation int64) string {
	if generation <= 0 {
		return ""
	}
	return strconv.FormatInt(generation, 10)
}

func objectChecksum(attributes *storage.ObjectAttrs) string {
	if len(attributes.MD5) != 0 {
		return "MD5:" + base64.StdEncoding.EncodeToString(attributes.MD5)
	}
	if attributes.CRC32C == 0 {
		return ""
	}
	return readerChecksum(attributes.CRC32C)
}

func readerChecksum(value uint32) string {
	if value == 0 {
		return ""
	}
	encoded := make([]byte, 4)
	binary.BigEndian.PutUint32(encoded, value)
	return "CRC32C:" + base64.StdEncoding.EncodeToString(encoded)
}

func cloneMetadata(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}

func cloneMetadataForUpdate(values map[string]string) map[string]string {
	if len(values) == 0 {
		return map[string]string{}
	}
	return cloneMetadata(values)
}

func emulatorServiceEndpoint(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return endpoint
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/storage/v1/"
	return parsed.String()
}

func normalizeError(err error) error {
	if errors.Is(err, storage.ErrObjectNotExist) || errors.Is(err, storage.ErrBucketNotExist) {
		return errors.Join(backend.ErrNotFound, err)
	}
	var responseError *googleapi.Error
	if !errors.As(err, &responseError) {
		return err
	}
	switch responseError.Code {
	case http.StatusNotFound:
		return errors.Join(backend.ErrNotFound, err)
	case http.StatusConflict, http.StatusPreconditionFailed:
		return errors.Join(backend.ErrConflict, err)
	default:
		return err
	}
}

var _ backend.Store = (*Store)(nil)
