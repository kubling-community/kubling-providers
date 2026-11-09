// Package azureblobstore implements the portable backend using Azure Blob Storage.
package azureblobstore

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	providerconfig "github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
)

// Store is safe for concurrent use.
type Store struct {
	client              *azblob.Client
	uploadBlockSize     int64
	transferConcurrency int
	sourceCredential    azcore.TokenCredential
	sharedKeySource     bool
}

const maxPutBlobFromURLSize = int64(5000 * 1024 * 1024)

// New creates an Azure Blob backend. Credentials remain outside the provider
// YAML and are resolved through DefaultAzureCredential or one named environment
// variable for explicit shared-key configurations.
func New(configuration providerconfig.Backend) (*Store, error) {
	if configuration.Type != "azure-blob" {
		return nil, fmt.Errorf("Azure Blob backend received type %q", configuration.Type)
	}

	var client *azblob.Client
	var sourceCredential azcore.TokenCredential
	sharedKeySource := false
	switch configuration.Authentication {
	case "default":
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("create default Azure credential: %w", err)
		}
		client, err = azblob.NewClient(configuration.AccountURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("create Azure Blob client: %w", err)
		}
		sourceCredential = credential
	case "shared-key":
		key, found := os.LookupEnv(configuration.AccountKeyEnvironment)
		if !found || strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf(
				"Azure account key environment variable %q is not set",
				configuration.AccountKeyEnvironment,
			)
		}
		credential, err := azblob.NewSharedKeyCredential(configuration.AccountName, key)
		if err != nil {
			return nil, fmt.Errorf("create Azure shared-key credential: %w", err)
		}
		client, err = azblob.NewClientWithSharedKeyCredential(
			configuration.AccountURL,
			credential,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("create Azure Blob client: %w", err)
		}
		sharedKeySource = true
	default:
		return nil, fmt.Errorf("unsupported Azure authentication %q", configuration.Authentication)
	}

	store := newWithClient(
		client,
		configuration.UploadBlockSizeBytes,
		configuration.TransferConcurrency,
	)
	store.sourceCredential = sourceCredential
	store.sharedKeySource = sharedKeySource
	return store, nil
}

func newWithClient(client *azblob.Client, blockSize int64, concurrency int) *Store {
	return &Store{
		client:              client,
		uploadBlockSize:     blockSize,
		transferConcurrency: concurrency,
	}
}

// Probe verifies access to one configured container without listing its blobs.
func (s *Store) Probe(ctx context.Context, bucket backend.Bucket) error {
	_, err := s.client.ServiceClient().NewContainerClient(bucket.Name).GetProperties(ctx, nil)
	if err != nil {
		return fmt.Errorf("probe Azure Blob container: %w", normalizeError(err))
	}
	return nil
}

// List returns one native Azure page below the configured root prefix.
func (s *Store) List(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.ListRequest,
) (backend.ListPage, error) {
	if request.Limit < 0 {
		return backend.ListPage{}, errors.New("Azure Blob list limit must not be negative")
	}
	prefix := bucket.Prefix + request.Prefix
	options := &container.ListBlobsFlatOptions{
		Include: container.ListBlobsInclude{Metadata: true},
		Prefix:  optionalString(prefix),
		Marker:  optionalString(request.ContinuationToken),
	}
	if request.Limit > 0 {
		options.MaxResults = &request.Limit
	}
	if request.StartAfter != "" {
		startFrom, err := backend.PhysicalKey(bucket, request.StartAfter)
		if err != nil {
			return backend.ListPage{}, err
		}
		options.StartFrom = &startFrom
	}

	pager := s.client.NewListBlobsFlatPager(bucket.Name, options)
	output, err := pager.NextPage(ctx)
	if err != nil {
		return backend.ListPage{}, fmt.Errorf("list Azure blobs: %w", normalizeError(err))
	}
	page := backend.ListPage{
		ContinuationToken: stringValue(output.NextMarker),
		Fields:            backend.AllObjectFields,
	}
	if output.Segment == nil {
		return page, nil
	}
	page.Objects = make([]backend.Object, 0, len(output.Segment.BlobItems))
	for _, item := range output.Segment.BlobItems {
		if item == nil || item.Properties == nil {
			continue
		}
		logicalKey, ok := backend.LogicalKey(bucket, stringValue(item.Name))
		if !ok || logicalKey == request.StartAfter {
			continue
		}
		etag := etagValue(item.Properties.ETag)
		page.Objects = append(page.Objects, backend.Object{
			Key:                logicalKey,
			Size:               int64Value(item.Properties.ContentLength),
			ETag:               etag,
			Revision:           etag,
			LastModified:       timeValue(item.Properties.LastModified),
			ContentType:        stringValue(item.Properties.ContentType),
			ContentEncoding:    stringValue(item.Properties.ContentEncoding),
			ContentDisposition: stringValue(item.Properties.ContentDisposition),
			CacheControl:       stringValue(item.Properties.CacheControl),
			VersionID:          stringValue(item.VersionID),
			Checksum:           md5Checksum(item.Properties.ContentMD5),
			Metadata:           metadataValues(item.Metadata),
		})
	}
	return page, nil
}

// Stat reads blob properties without downloading content.
func (s *Store) Stat(
	ctx context.Context,
	bucket backend.Bucket,
	key string,
) (backend.Object, error) {
	physicalKey, err := backend.PhysicalKey(bucket, key)
	if err != nil {
		return backend.Object{}, err
	}
	output, err := s.client.ServiceClient().NewContainerClient(bucket.Name).
		NewBlobClient(physicalKey).GetProperties(ctx, nil)
	if err != nil {
		err = normalizeError(err)
		if errors.Is(err, backend.ErrNotFound) {
			return backend.Object{}, err
		}
		return backend.Object{}, fmt.Errorf("stat Azure blob: %w", err)
	}
	return objectFromProperties(key, output), nil
}

// Open streams one complete blob or byte range.
func (s *Store) Open(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.GetRequest,
) (backend.ObjectReader, error) {
	if request.Offset < 0 {
		return backend.ObjectReader{}, errors.New("Azure blob offset must not be negative")
	}
	if request.Length != nil && *request.Length < 0 {
		return backend.ObjectReader{}, errors.New("Azure blob length must not be negative")
	}
	if request.Length != nil && *request.Length == 0 {
		object, err := s.Stat(ctx, bucket, request.Key)
		if err != nil {
			return backend.ObjectReader{}, err
		}
		return backend.ObjectReader{Object: object, Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	physicalKey, err := backend.PhysicalKey(bucket, request.Key)
	if err != nil {
		return backend.ObjectReader{}, err
	}
	client := s.client.ServiceClient().NewContainerClient(bucket.Name).NewBlobClient(physicalKey)
	if request.VersionID != "" {
		client, err = client.WithVersionID(request.VersionID)
		if err != nil {
			return backend.ObjectReader{}, fmt.Errorf("select Azure blob version: %w", err)
		}
	}
	options := &blob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: request.Offset},
	}
	if request.Length != nil {
		options.Range.Count = *request.Length
	}
	if request.Revision != "" {
		options.AccessConditions = accessConditions(request.Revision, false)
	}
	output, err := client.DownloadStream(ctx, options)
	if err != nil {
		err = normalizeError(err)
		if errors.Is(err, backend.ErrNotFound) || errors.Is(err, backend.ErrConflict) {
			return backend.ObjectReader{}, err
		}
		return backend.ObjectReader{}, fmt.Errorf("open Azure blob: %w", err)
	}
	if output.Body == nil {
		return backend.ObjectReader{}, errors.New("open Azure blob: backend returned a nil body")
	}
	return backend.ObjectReader{
		Object: objectFromDownload(request.Key, output),
		Body:   output.NewRetryReader(ctx, nil),
	}, nil
}

// UpdateAttributes changes portable blob properties without sending the body
// through the provider. Azure exposes metadata and HTTP properties as separate
// operations, so mixed updates use one conditional in-place server rewrite.
func (s *Store) UpdateAttributes(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.UpdateAttributesRequest,
) error {
	if request.Fields.Empty() {
		return errors.New("Azure Blob attribute update requires at least one field")
	}
	if request.IfRevisionMatch == "" {
		return errors.New("Azure Blob attribute update requires a revision")
	}
	if request.Size < 0 {
		return errors.New("Azure blob size must not be negative")
	}
	physicalKey, err := backend.PhysicalKey(bucket, request.Key)
	if err != nil {
		return err
	}
	client := s.client.ServiceClient().NewContainerClient(bucket.Name).NewBlockBlobClient(physicalKey)
	headersAssigned := request.Fields.HTTPHeaders()

	if request.Fields.Metadata && !headersAssigned {
		_, err = client.SetMetadata(ctx, metadataPointers(request.Metadata), &blob.SetMetadataOptions{
			AccessConditions: accessConditions(request.IfRevisionMatch, false),
		})
		return normalizeAttributeError("update Azure blob metadata", err)
	}
	if headersAssigned && !request.Fields.Metadata {
		_, err = client.SetHTTPHeaders(ctx, attributeHTTPHeaders(request), &blob.SetHTTPHeadersOptions{
			AccessConditions: accessConditions(request.IfRevisionMatch, false),
		})
		return normalizeAttributeError("update Azure blob properties", err)
	}
	if request.Size > maxPutBlobFromURLSize {
		return fmt.Errorf(
			"Azure Blob cannot atomically update metadata and HTTP properties together for objects larger than 5000 MiB",
		)
	}
	sourceURL, sourceAuthorization, err := s.copySource(ctx, client)
	if err != nil {
		return err
	}
	copySourceProperties := false
	match := quotedETag(request.IfRevisionMatch)
	headers := attributeHTTPHeaders(request)
	_, err = client.UploadBlobFromURL(ctx, sourceURL, &blockblob.UploadBlobFromURLOptions{
		CopySourceAuthorization:  sourceAuthorization,
		CopySourceBlobProperties: &copySourceProperties,
		HTTPHeaders:              &headers,
		Metadata:                 metadataPointers(request.Metadata),
		AccessConditions:         accessConditions(request.IfRevisionMatch, false),
		SourceModifiedAccessConditions: &blob.SourceModifiedAccessConditions{
			SourceIfMatch: &match,
		},
	})
	return normalizeAttributeError("rewrite Azure blob attributes", err)
}

func (s *Store) copySource(
	ctx context.Context,
	client *blockblob.Client,
) (string, *string, error) {
	if s.sourceCredential != nil {
		token, err := s.sourceCredential.GetToken(ctx, policy.TokenRequestOptions{
			Scopes: []string{"https://storage.azure.com/.default"},
		})
		if err != nil {
			return "", nil, fmt.Errorf("authorize Azure blob copy source: %w", err)
		}
		authorization := "Bearer " + token.Token
		return client.URL(), &authorization, nil
	}
	if s.sharedKeySource {
		sourceURL, err := client.GetSASURL(
			sas.BlobPermissions{Read: true},
			time.Now().UTC().Add(5*time.Minute),
			nil,
		)
		if err != nil {
			return "", nil, fmt.Errorf("authorize Azure blob copy source: %w", err)
		}
		return sourceURL, nil, nil
	}
	return client.URL(), nil, nil
}

func attributeHTTPHeaders(request backend.UpdateAttributesRequest) blob.HTTPHeaders {
	return blob.HTTPHeaders{
		BlobContentType:        optionalString(request.ContentType),
		BlobContentEncoding:    optionalString(request.ContentEncoding),
		BlobContentDisposition: optionalString(request.ContentDisposition),
		BlobCacheControl:       optionalString(request.CacheControl),
	}
}

func normalizeAttributeError(operation string, err error) error {
	if err == nil {
		return nil
	}
	err = normalizeError(err)
	if errors.Is(err, backend.ErrNotFound) || errors.Is(err, backend.ErrConflict) {
		return err
	}
	return fmt.Errorf("%s: %w", operation, err)
}

// Put uploads one complete replacement block blob.
func (s *Store) Put(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.PutRequest,
) (backend.Object, error) {
	if request.Body == nil {
		return backend.Object{}, errors.New("Azure blob body is required")
	}
	if request.Size != nil && *request.Size < 0 {
		return backend.Object{}, errors.New("Azure blob size must not be negative")
	}
	if request.IfNoneMatch && request.IfRevisionMatch != "" {
		return backend.Object{}, errors.New("Azure blob put cannot combine revision and nonexistence conditions")
	}
	physicalKey, err := backend.PhysicalKey(bucket, request.Key)
	if err != nil {
		return backend.Object{}, err
	}
	options := &azblob.UploadStreamOptions{
		BlockSize:   s.uploadBlockSize,
		Concurrency: s.transferConcurrency,
		HTTPHeaders: &blob.HTTPHeaders{
			BlobContentType:        optionalString(request.ContentType),
			BlobContentEncoding:    optionalString(request.ContentEncoding),
			BlobContentDisposition: optionalString(request.ContentDisposition),
			BlobCacheControl:       optionalString(request.CacheControl),
		},
		Metadata: metadataPointers(request.Metadata),
	}
	if request.IfRevisionMatch != "" || request.IfNoneMatch {
		options.AccessConditions = accessConditions(request.IfRevisionMatch, request.IfNoneMatch)
	}
	output, err := s.client.UploadStream(ctx, bucket.Name, physicalKey, request.Body, options)
	if err != nil {
		err = normalizeError(err)
		if errors.Is(err, backend.ErrConflict) {
			return backend.Object{}, err
		}
		return backend.Object{}, fmt.Errorf("upload Azure blob: %w", err)
	}
	size := int64(0)
	if request.Size != nil {
		size = *request.Size
	}
	etag := etagValue(output.ETag)
	return backend.Object{
		Key:                request.Key,
		Size:               size,
		ETag:               etag,
		Revision:           etag,
		LastModified:       timeValue(output.LastModified),
		ContentType:        request.ContentType,
		ContentEncoding:    request.ContentEncoding,
		ContentDisposition: request.ContentDisposition,
		CacheControl:       request.CacheControl,
		VersionID:          stringValue(output.VersionID),
		Checksum:           responseChecksum(output.ContentMD5, output.ContentCRC64),
		Metadata:           cloneMetadata(request.Metadata),
	}, nil
}

// Delete removes one blob version under an optional revision condition.
func (s *Store) Delete(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.DeleteRequest,
) error {
	physicalKey, err := backend.PhysicalKey(bucket, request.Key)
	if err != nil {
		return err
	}
	client := s.client.ServiceClient().NewContainerClient(bucket.Name).NewBlobClient(physicalKey)
	if request.VersionID != "" {
		client, err = client.WithVersionID(request.VersionID)
		if err != nil {
			return fmt.Errorf("select Azure blob version: %w", err)
		}
	}
	options := &blob.DeleteOptions{}
	if request.IfRevisionMatch != "" {
		options.AccessConditions = accessConditions(request.IfRevisionMatch, false)
	}
	_, err = client.Delete(ctx, options)
	if err != nil {
		err = normalizeError(err)
		if errors.Is(err, backend.ErrNotFound) || errors.Is(err, backend.ErrConflict) {
			return err
		}
		return fmt.Errorf("delete Azure blob: %w", err)
	}
	return nil
}

// Close releases no resources; Azure clients own reusable HTTP transports.
func (s *Store) Close() error {
	return nil
}

func objectFromProperties(key string, output blob.GetPropertiesResponse) backend.Object {
	etag := etagValue(output.ETag)
	return backend.Object{
		Key:                key,
		Size:               int64Value(output.ContentLength),
		ETag:               etag,
		Revision:           etag,
		LastModified:       timeValue(output.LastModified),
		ContentType:        stringValue(output.ContentType),
		ContentEncoding:    stringValue(output.ContentEncoding),
		ContentDisposition: stringValue(output.ContentDisposition),
		CacheControl:       stringValue(output.CacheControl),
		VersionID:          stringValue(output.VersionID),
		Checksum:           md5Checksum(output.ContentMD5),
		Metadata:           metadataValues(output.Metadata),
	}
}

func objectFromDownload(key string, output blob.DownloadStreamResponse) backend.Object {
	etag := etagValue(output.ETag)
	return backend.Object{
		Key:                key,
		Size:               totalSize(stringValue(output.ContentRange), int64Value(output.ContentLength)),
		ETag:               etag,
		Revision:           etag,
		LastModified:       timeValue(output.LastModified),
		ContentType:        stringValue(output.ContentType),
		ContentEncoding:    stringValue(output.ContentEncoding),
		ContentDisposition: stringValue(output.ContentDisposition),
		CacheControl:       stringValue(output.CacheControl),
		VersionID:          stringValue(output.VersionID),
		Checksum:           responseChecksum(output.BlobContentMD5, output.ContentCRC64),
		Metadata:           metadataValues(output.Metadata),
	}
}

func accessConditions(revision string, mustNotExist bool) *blob.AccessConditions {
	conditions := &blob.ModifiedAccessConditions{}
	if mustNotExist {
		value := azcore.ETagAny
		conditions.IfNoneMatch = &value
	} else if revision != "" {
		value := quotedETag(revision)
		conditions.IfMatch = &value
	}
	return &blob.AccessConditions{ModifiedAccessConditions: conditions}
}

func quotedETag(value string) azcore.ETag {
	return azcore.ETag("\"" + strings.Trim(value, "\"") + "\"")
}

func etagValue(value *azcore.ETag) string {
	if value == nil {
		return ""
	}
	return strings.Trim(string(*value), "\"")
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func int64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func timeValue(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return *value
}

func metadataPointers(values map[string]string) map[string]*string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]*string, len(values))
	for key, value := range values {
		copy := value
		result[strings.ToLower(key)] = &copy
	}
	return result
}

func metadataValues(values map[string]*string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[strings.ToLower(key)] = stringValue(value)
	}
	return result
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

func md5Checksum(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	return "MD5:" + base64.StdEncoding.EncodeToString(value)
}

func responseChecksum(md5 []byte, crc64 []byte) string {
	if len(md5) != 0 {
		return md5Checksum(md5)
	}
	if len(crc64) != 0 {
		return "CRC64:" + base64.StdEncoding.EncodeToString(crc64)
	}
	return ""
}

func totalSize(contentRange string, fallback int64) int64 {
	_, total, found := strings.Cut(contentRange, "/")
	if !found || total == "*" {
		return fallback
	}
	value, err := strconv.ParseInt(total, 10, 64)
	if err != nil || value < 0 {
		return fallback
	}
	return value
}

func normalizeError(err error) error {
	var responseError *azcore.ResponseError
	if !errors.As(err, &responseError) {
		return err
	}
	switch responseError.StatusCode {
	case http.StatusNotFound:
		return errors.Join(backend.ErrNotFound, err)
	case http.StatusConflict, http.StatusPreconditionFailed:
		return errors.Join(backend.ErrConflict, err)
	default:
		return err
	}
}

var _ backend.Store = (*Store)(nil)
