// Package s3store implements the portable backend using the AWS S3 API.
package s3store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	providerconfig "github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
)

type s3API interface {
	HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	GetObjectAcl(context.Context, *s3.GetObjectAclInput, ...func(*s3.Options)) (*s3.GetObjectAclOutput, error)
	GetObjectTagging(context.Context, *s3.GetObjectTaggingInput, ...func(*s3.Options)) (*s3.GetObjectTaggingOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPartCopy(context.Context, *s3.UploadPartCopyInput, ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type uploadAPI interface {
	UploadObject(
		context.Context,
		*transfermanager.UploadObjectInput,
		...func(*transfermanager.Options),
	) (*transfermanager.UploadObjectOutput, error)
}

// Store is safe for concurrent use.
type Store struct {
	client       s3API
	uploader     uploadAPI
	copyPartSize int64
}

type copyAttributes struct {
	contentType             *string
	contentEncoding         *string
	contentDisposition      *string
	contentLanguage         *string
	cacheControl            *string
	expires                 *time.Time
	metadata                map[string]string
	storageClass            s3types.StorageClass
	serverSideEncryption    s3types.ServerSideEncryption
	sseKMSKeyID             *string
	bucketKeyEnabled        *bool
	websiteRedirectLocation *string
	objectLockMode          s3types.ObjectLockMode
	objectLockRetainUntil   *time.Time
	objectLockLegalHold     s3types.ObjectLockLegalHoldStatus
	grantFullControl        *string
	grantRead               *string
	grantReadACP            *string
	grantWriteACP           *string
	tagging                 *string
}

type aclGrantHeaders struct {
	fullControl *string
	read        *string
	readACP     *string
	writeACP    *string
}

const (
	maxSingleCopySize   = int64(5 * 1024 * 1024 * 1024)
	defaultCopyPartSize = int64(64 * 1024 * 1024)
	minimumCopyPartSize = int64(5 * 1024 * 1024)
	maximumCopyPartSize = int64(5 * 1024 * 1024 * 1024)
	maximumCopyParts    = int64(10_000)
)

// New loads the standard AWS credential chain and creates an S3 backend.
//
// Credentials are intentionally not accepted by the provider YAML. Workload
// identity, environment variables and AWS shared configuration remain owned by
// the SDK credential chain.
func New(ctx context.Context, configuration providerconfig.Backend) (*Store, error) {
	loadOptions := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(configuration.Region),
	}
	if configuration.Profile != "" {
		loadOptions = append(
			loadOptions,
			awsconfig.WithSharedConfigProfile(configuration.Profile),
		)
	}
	awsConfiguration, err := awsconfig.LoadDefaultConfig(ctx, loadOptions...)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}

	client := s3.NewFromConfig(awsConfiguration, func(options *s3.Options) {
		options.UsePathStyle = configuration.ForcePathStyle
		if configuration.Endpoint != "" {
			options.BaseEndpoint = aws.String(configuration.Endpoint)
		}
	})
	uploader := transfermanager.New(client, func(options *transfermanager.Options) {
		if configuration.UploadPartSizeBytes > 0 {
			options.PartSizeBytes = configuration.UploadPartSizeBytes
		}
		if configuration.MultipartThresholdBytes > 0 {
			options.MultipartUploadThreshold = configuration.MultipartThresholdBytes
		}
		if configuration.TransferConcurrency > 0 {
			options.Concurrency = configuration.TransferConcurrency
		}
	})
	copyPartSize := configuration.UploadPartSizeBytes
	if copyPartSize == 0 {
		copyPartSize = defaultCopyPartSize
	}
	return &Store{client: client, uploader: uploader, copyPartSize: copyPartSize}, nil
}

func newWithClients(client s3API, uploader uploadAPI) *Store {
	return &Store{client: client, uploader: uploader, copyPartSize: defaultCopyPartSize}
}

// Probe verifies access to one configured bucket without listing its objects.
func (s *Store) Probe(ctx context.Context, bucket backend.Bucket) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket.Name),
	})
	if err != nil {
		return fmt.Errorf("probe S3 bucket: %w", err)
	}
	return nil
}

// List returns one native S3 page below the configured root prefix.
func (s *Store) List(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.ListRequest,
) (backend.ListPage, error) {
	if request.Limit < 0 {
		return backend.ListPage{}, errors.New("S3 list limit must not be negative")
	}
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket.Name),
		Prefix: optionalString(bucket.Prefix + request.Prefix),
	}
	if request.ContinuationToken != "" {
		input.ContinuationToken = aws.String(request.ContinuationToken)
	}
	if request.StartAfter != "" {
		startAfter, err := backend.PhysicalKey(bucket, request.StartAfter)
		if err != nil {
			return backend.ListPage{}, err
		}
		input.StartAfter = aws.String(startAfter)
	}
	if request.Limit > 0 {
		input.MaxKeys = aws.Int32(request.Limit)
	}

	output, err := s.client.ListObjectsV2(ctx, input)
	if err != nil {
		return backend.ListPage{}, fmt.Errorf("list S3 objects: %w", err)
	}
	page := backend.ListPage{
		Objects:           make([]backend.Object, 0, len(output.Contents)),
		ContinuationToken: aws.ToString(output.NextContinuationToken),
		Fields: backend.ObjectFieldKey |
			backend.ObjectFieldContent |
			backend.ObjectFieldContentLength |
			backend.ObjectFieldETag |
			backend.ObjectFieldLastModified,
	}
	for _, item := range output.Contents {
		logicalKey, ok := backend.LogicalKey(bucket, aws.ToString(item.Key))
		if !ok {
			continue
		}
		page.Objects = append(page.Objects, backend.Object{
			Key:          logicalKey,
			Size:         aws.ToInt64(item.Size),
			ETag:         trimETag(aws.ToString(item.ETag)),
			Revision:     trimETag(aws.ToString(item.ETag)),
			LastModified: aws.ToTime(item.LastModified),
		})
	}
	return page, nil
}

// Stat reads object headers without downloading content.
func (s *Store) Stat(
	ctx context.Context,
	bucket backend.Bucket,
	key string,
) (backend.Object, error) {
	physicalKey, err := backend.PhysicalKey(bucket, key)
	if err != nil {
		return backend.Object{}, err
	}
	output, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket.Name),
		Key:    aws.String(physicalKey),
	})
	if err != nil {
		if isNotFound(err) {
			return backend.Object{}, backend.ErrNotFound
		}
		return backend.Object{}, fmt.Errorf("stat S3 object: %w", err)
	}
	return objectFromHead(key, output), nil
}

// Open streams one complete object or byte range.
func (s *Store) Open(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.GetRequest,
) (backend.ObjectReader, error) {
	if request.Offset < 0 {
		return backend.ObjectReader{}, errors.New("S3 object offset must not be negative")
	}
	if request.Length != nil && *request.Length < 0 {
		return backend.ObjectReader{}, errors.New("S3 object length must not be negative")
	}
	if request.Length != nil && *request.Length == 0 {
		object, err := s.Stat(ctx, bucket, request.Key)
		if err != nil {
			return backend.ObjectReader{}, err
		}
		return backend.ObjectReader{
			Object: object,
			Body:   io.NopCloser(strings.NewReader("")),
		}, nil
	}

	physicalKey, err := backend.PhysicalKey(bucket, request.Key)
	if err != nil {
		return backend.ObjectReader{}, err
	}
	input := &s3.GetObjectInput{
		Bucket:    aws.String(bucket.Name),
		Key:       aws.String(physicalKey),
		VersionId: optionalString(request.VersionID),
		IfMatch:   optionalQuotedETag(request.Revision),
	}
	if request.Offset > 0 || request.Length != nil {
		input.Range = aws.String(byteRange(request.Offset, request.Length))
	}
	output, err := s.client.GetObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			return backend.ObjectReader{}, backend.ErrNotFound
		}
		return backend.ObjectReader{}, fmt.Errorf("open S3 object: %w", err)
	}
	if output.Body == nil {
		return backend.ObjectReader{}, errors.New("open S3 object: backend returned a nil body")
	}
	return backend.ObjectReader{
		Object: objectFromGet(request.Key, output),
		Body:   output.Body,
	}, nil
}

// UpdateAttributes replaces portable object attributes through a server-side
// self-copy. Large objects use multipart copy so their body never crosses the
// provider process.
func (s *Store) UpdateAttributes(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.UpdateAttributesRequest,
) error {
	if request.Fields.Empty() {
		return errors.New("S3 attribute update requires at least one field")
	}
	if request.IfRevisionMatch == "" {
		return errors.New("S3 attribute update requires a revision")
	}
	if request.Size < 0 {
		return errors.New("S3 object size must not be negative")
	}
	physicalKey, err := backend.PhysicalKey(bucket, request.Key)
	if err != nil {
		return err
	}
	attributes, size, err := s.copyAttributes(
		ctx,
		bucket.Name,
		physicalKey,
		request,
		request.Size > maxSingleCopySize,
	)
	if err != nil {
		return err
	}
	copySource := url.PathEscape(bucket.Name + "/" + physicalKey)
	if size <= maxSingleCopySize {
		_, err = s.client.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:                    aws.String(bucket.Name),
			Key:                       aws.String(physicalKey),
			CopySource:                aws.String(copySource),
			CopySourceIfMatch:         optionalQuotedETag(request.IfRevisionMatch),
			IfMatch:                   optionalQuotedETag(request.IfRevisionMatch),
			ContentType:               attributes.contentType,
			ContentEncoding:           attributes.contentEncoding,
			ContentDisposition:        attributes.contentDisposition,
			ContentLanguage:           attributes.contentLanguage,
			CacheControl:              attributes.cacheControl,
			Expires:                   attributes.expires,
			Metadata:                  attributes.metadata,
			MetadataDirective:         s3types.MetadataDirectiveReplace,
			StorageClass:              attributes.storageClass,
			ServerSideEncryption:      attributes.serverSideEncryption,
			SSEKMSKeyId:               attributes.sseKMSKeyID,
			BucketKeyEnabled:          attributes.bucketKeyEnabled,
			WebsiteRedirectLocation:   attributes.websiteRedirectLocation,
			ObjectLockMode:            attributes.objectLockMode,
			ObjectLockRetainUntilDate: attributes.objectLockRetainUntil,
			ObjectLockLegalHoldStatus: attributes.objectLockLegalHold,
			GrantFullControl:          attributes.grantFullControl,
			GrantRead:                 attributes.grantRead,
			GrantReadACP:              attributes.grantReadACP,
			GrantWriteACP:             attributes.grantWriteACP,
		})
		if err != nil {
			return normalizeMutationError("copy S3 object attributes", err)
		}
		return nil
	}
	request.Size = size
	return s.updateAttributesMultipart(
		ctx,
		bucket.Name,
		physicalKey,
		copySource,
		request,
		attributes,
	)
}

func (s *Store) copyAttributes(
	ctx context.Context,
	bucketName string,
	physicalKey string,
	request backend.UpdateAttributesRequest,
	includeTags bool,
) (copyAttributes, int64, error) {
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:  aws.String(bucketName),
		Key:     aws.String(physicalKey),
		IfMatch: optionalQuotedETag(request.IfRevisionMatch),
	})
	if err != nil {
		return copyAttributes{}, 0, normalizeMutationError("read S3 object attributes", err)
	}
	attributes := copyAttributes{
		contentType:             head.ContentType,
		contentEncoding:         head.ContentEncoding,
		contentDisposition:      head.ContentDisposition,
		contentLanguage:         head.ContentLanguage,
		cacheControl:            head.CacheControl,
		expires:                 head.Expires,
		metadata:                cloneMetadata(head.Metadata),
		storageClass:            head.StorageClass,
		serverSideEncryption:    head.ServerSideEncryption,
		sseKMSKeyID:             head.SSEKMSKeyId,
		bucketKeyEnabled:        head.BucketKeyEnabled,
		websiteRedirectLocation: head.WebsiteRedirectLocation,
		objectLockMode:          head.ObjectLockMode,
		objectLockRetainUntil:   head.ObjectLockRetainUntilDate,
		objectLockLegalHold:     head.ObjectLockLegalHoldStatus,
	}
	if request.Fields.ContentType {
		attributes.contentType = optionalString(request.ContentType)
	}
	if request.Fields.ContentEncoding {
		attributes.contentEncoding = optionalString(request.ContentEncoding)
	}
	if request.Fields.ContentDisposition {
		attributes.contentDisposition = optionalString(request.ContentDisposition)
	}
	if request.Fields.CacheControl {
		attributes.cacheControl = optionalString(request.CacheControl)
	}
	if request.Fields.Metadata {
		attributes.metadata = cloneMetadata(request.Metadata)
	}

	acl, err := s.client.GetObjectAcl(ctx, &s3.GetObjectAclInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(physicalKey),
		VersionId: head.VersionId,
	})
	if err != nil {
		if !isNotImplemented(err) {
			return copyAttributes{}, 0, normalizeMutationError("read S3 object ACL", err)
		}
	} else {
		grants, err := objectACLGrantHeaders(acl)
		if err != nil {
			return copyAttributes{}, 0, err
		}
		attributes.grantFullControl = grants.fullControl
		attributes.grantRead = grants.read
		attributes.grantReadACP = grants.readACP
		attributes.grantWriteACP = grants.writeACP
	}

	if includeTags {
		tags, err := s.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket:    aws.String(bucketName),
			Key:       aws.String(physicalKey),
			VersionId: head.VersionId,
		})
		if err != nil {
			return copyAttributes{}, 0, normalizeMutationError("read S3 object tags", err)
		}
		attributes.tagging, err = encodedTagSet(tags.TagSet)
		if err != nil {
			return copyAttributes{}, 0, err
		}
	}
	return attributes, aws.ToInt64(head.ContentLength), nil
}

func (s *Store) updateAttributesMultipart(
	ctx context.Context,
	bucketName string,
	physicalKey string,
	copySource string,
	request backend.UpdateAttributesRequest,
	attributes copyAttributes,
) (resultErr error) {
	partSize, err := multipartCopyPartSize(request.Size, s.copyPartSize)
	if err != nil {
		return err
	}
	created, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket:                    aws.String(bucketName),
		Key:                       aws.String(physicalKey),
		ContentType:               attributes.contentType,
		ContentEncoding:           attributes.contentEncoding,
		ContentDisposition:        attributes.contentDisposition,
		ContentLanguage:           attributes.contentLanguage,
		CacheControl:              attributes.cacheControl,
		Expires:                   attributes.expires,
		Metadata:                  attributes.metadata,
		StorageClass:              attributes.storageClass,
		ServerSideEncryption:      attributes.serverSideEncryption,
		SSEKMSKeyId:               attributes.sseKMSKeyID,
		BucketKeyEnabled:          attributes.bucketKeyEnabled,
		WebsiteRedirectLocation:   attributes.websiteRedirectLocation,
		ObjectLockMode:            attributes.objectLockMode,
		ObjectLockRetainUntilDate: attributes.objectLockRetainUntil,
		ObjectLockLegalHoldStatus: attributes.objectLockLegalHold,
		GrantFullControl:          attributes.grantFullControl,
		GrantRead:                 attributes.grantRead,
		GrantReadACP:              attributes.grantReadACP,
		GrantWriteACP:             attributes.grantWriteACP,
		Tagging:                   attributes.tagging,
	})
	if err != nil {
		return fmt.Errorf("begin multipart S3 attribute copy: %w", err)
	}
	uploadID := aws.ToString(created.UploadId)
	if uploadID == "" {
		return errors.New("begin multipart S3 attribute copy: backend returned no upload id")
	}
	completed := false
	defer func() {
		if completed {
			return
		}
		_, _ = s.client.AbortMultipartUpload(context.WithoutCancel(ctx), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(physicalKey),
			UploadId: aws.String(uploadID),
		})
	}()

	parts := make([]s3types.CompletedPart, 0, (request.Size+partSize-1)/partSize)
	for offset, partNumber := int64(0), int32(1); offset < request.Size; offset, partNumber = offset+partSize, partNumber+1 {
		end := min(offset+partSize, request.Size) - 1
		output, err := s.client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:            aws.String(bucketName),
			Key:               aws.String(physicalKey),
			UploadId:          aws.String(uploadID),
			PartNumber:        aws.Int32(partNumber),
			CopySource:        aws.String(copySource),
			CopySourceIfMatch: optionalQuotedETag(request.IfRevisionMatch),
			CopySourceRange:   aws.String(fmt.Sprintf("bytes=%d-%d", offset, end)),
		})
		if err != nil {
			return normalizeMutationError("copy multipart S3 object part", err)
		}
		if output.CopyPartResult == nil || output.CopyPartResult.ETag == nil {
			return errors.New("copy multipart S3 object part: backend returned no ETag")
		}
		parts = append(parts, s3types.CompletedPart{
			ETag:       output.CopyPartResult.ETag,
			PartNumber: aws.Int32(partNumber),
		})
	}
	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(physicalKey),
		UploadId: aws.String(uploadID),
		IfMatch:  optionalQuotedETag(request.IfRevisionMatch),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return normalizeMutationError("complete multipart S3 attribute copy", err)
	}
	completed = true
	return nil
}

func multipartCopyPartSize(size int64, configured int64) (int64, error) {
	partSize := configured
	if partSize < minimumCopyPartSize {
		partSize = minimumCopyPartSize
	}
	minimumForPartCount := (size + maximumCopyParts - 1) / maximumCopyParts
	if minimumForPartCount > partSize {
		partSize = ((minimumForPartCount + minimumCopyPartSize - 1) / minimumCopyPartSize) * minimumCopyPartSize
	}
	if partSize > maximumCopyPartSize {
		return 0, errors.New("S3 object is too large for multipart attribute copy")
	}
	return partSize, nil
}

func objectACLGrantHeaders(output *s3.GetObjectAclOutput) (aclGrantHeaders, error) {
	if output == nil {
		return aclGrantHeaders{}, errors.New("read S3 object ACL: backend returned no ACL")
	}
	if privateObjectACL(output) {
		return aclGrantHeaders{}, nil
	}

	values := map[s3types.Permission][]string{}
	for _, grant := range output.Grants {
		if grant.Grantee == nil {
			return aclGrantHeaders{}, errors.New("read S3 object ACL: grant has no grantee")
		}
		grantee, err := encodedACLGrantee(grant.Grantee)
		if err != nil {
			return aclGrantHeaders{}, err
		}
		switch grant.Permission {
		case s3types.PermissionFullControl,
			s3types.PermissionRead,
			s3types.PermissionReadAcp,
			s3types.PermissionWriteAcp:
			values[grant.Permission] = append(values[grant.Permission], grantee)
		default:
			return aclGrantHeaders{}, fmt.Errorf(
				"read S3 object ACL: cannot preserve %q permission",
				grant.Permission,
			)
		}
	}
	return aclGrantHeaders{
		fullControl: joinedGrantValues(values[s3types.PermissionFullControl]),
		read:        joinedGrantValues(values[s3types.PermissionRead]),
		readACP:     joinedGrantValues(values[s3types.PermissionReadAcp]),
		writeACP:    joinedGrantValues(values[s3types.PermissionWriteAcp]),
	}, nil
}

func privateObjectACL(output *s3.GetObjectAclOutput) bool {
	if len(output.Grants) == 0 {
		return true
	}
	if len(output.Grants) != 1 || output.Owner == nil || output.Owner.ID == nil {
		return false
	}
	grant := output.Grants[0]
	return grant.Permission == s3types.PermissionFullControl &&
		grant.Grantee != nil &&
		grant.Grantee.Type == s3types.TypeCanonicalUser &&
		aws.ToString(grant.Grantee.ID) == aws.ToString(output.Owner.ID)
}

func encodedACLGrantee(grantee *s3types.Grantee) (string, error) {
	var kind string
	var value string
	switch grantee.Type {
	case s3types.TypeCanonicalUser:
		kind, value = "id", aws.ToString(grantee.ID)
	case s3types.TypeGroup:
		kind, value = "uri", aws.ToString(grantee.URI)
	case s3types.TypeAmazonCustomerByEmail:
		kind, value = "emailAddress", aws.ToString(grantee.EmailAddress)
	default:
		return "", fmt.Errorf("read S3 object ACL: unsupported grantee type %q", grantee.Type)
	}
	if value == "" {
		return "", fmt.Errorf("read S3 object ACL: %s grantee has no value", kind)
	}
	return kind + "=" + strconv.Quote(value), nil
}

func joinedGrantValues(values []string) *string {
	if len(values) == 0 {
		return nil
	}
	sort.Strings(values)
	return aws.String(strings.Join(values, ", "))
}

func encodedTagSet(tags []s3types.Tag) (*string, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	values := url.Values{}
	for _, tag := range tags {
		if tag.Key == nil || tag.Value == nil {
			return nil, errors.New("read S3 object tags: tag has no key or value")
		}
		values.Add(aws.ToString(tag.Key), aws.ToString(tag.Value))
	}
	return aws.String(values.Encode()), nil
}

func normalizeMutationError(operation string, err error) error {
	if isConflict(err) {
		return backend.ErrConflict
	}
	if isNotFound(err) {
		return backend.ErrNotFound
	}
	return fmt.Errorf("%s: %w", operation, err)
}

// Put uploads one replacement object and automatically uses multipart upload
// when the configured transfer-manager threshold is crossed.
func (s *Store) Put(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.PutRequest,
) (backend.Object, error) {
	if request.Body == nil {
		return backend.Object{}, errors.New("S3 object body is required")
	}
	if request.Size != nil && *request.Size < 0 {
		return backend.Object{}, errors.New("S3 object size must not be negative")
	}
	physicalKey, err := backend.PhysicalKey(bucket, request.Key)
	if err != nil {
		return backend.Object{}, err
	}

	input := &transfermanager.UploadObjectInput{
		Bucket:             aws.String(bucket.Name),
		Key:                aws.String(physicalKey),
		Body:               request.Body,
		ContentLength:      request.Size,
		MpuObjectSize:      request.Size,
		ContentType:        optionalString(request.ContentType),
		ContentEncoding:    optionalString(request.ContentEncoding),
		ContentDisposition: optionalString(request.ContentDisposition),
		CacheControl:       optionalString(request.CacheControl),
		Metadata:           cloneMetadata(request.Metadata),
		IfMatch:            optionalQuotedETag(request.IfRevisionMatch),
	}
	if request.IfNoneMatch {
		input.IfNoneMatch = aws.String("*")
	}
	output, err := s.uploader.UploadObject(ctx, input)
	if err != nil {
		if isConflict(err) {
			return backend.Object{}, backend.ErrConflict
		}
		return backend.Object{}, fmt.Errorf("upload S3 object: %w", err)
	}
	size := int64(0)
	if request.Size != nil {
		size = *request.Size
	} else if output.ContentLength != nil {
		size = *output.ContentLength
	}
	return backend.Object{
		Key:                request.Key,
		Size:               size,
		ETag:               trimETag(aws.ToString(output.ETag)),
		Revision:           trimETag(aws.ToString(output.ETag)),
		ContentType:        request.ContentType,
		ContentEncoding:    request.ContentEncoding,
		ContentDisposition: request.ContentDisposition,
		CacheControl:       request.CacheControl,
		VersionID:          aws.ToString(output.VersionID),
		Checksum:           uploadChecksum(output),
		Metadata:           cloneMetadata(request.Metadata),
	}, nil
}

// Delete removes one object or an explicitly selected version.
func (s *Store) Delete(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.DeleteRequest,
) error {
	physicalKey, err := backend.PhysicalKey(bucket, request.Key)
	if err != nil {
		return err
	}
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String(bucket.Name),
		Key:       aws.String(physicalKey),
		VersionId: optionalString(request.VersionID),
		IfMatch:   optionalQuotedETag(request.IfRevisionMatch),
	})
	if err != nil {
		return fmt.Errorf("delete S3 object: %w", err)
	}
	return nil
}

// Close releases no resources; AWS clients own reusable HTTP transports.
func (s *Store) Close() error {
	return nil
}

func objectFromHead(key string, output *s3.HeadObjectOutput) backend.Object {
	return backend.Object{
		Key:                key,
		Size:               aws.ToInt64(output.ContentLength),
		ETag:               trimETag(aws.ToString(output.ETag)),
		Revision:           trimETag(aws.ToString(output.ETag)),
		LastModified:       aws.ToTime(output.LastModified),
		ContentType:        aws.ToString(output.ContentType),
		ContentEncoding:    aws.ToString(output.ContentEncoding),
		ContentDisposition: aws.ToString(output.ContentDisposition),
		CacheControl:       aws.ToString(output.CacheControl),
		VersionID:          aws.ToString(output.VersionId),
		Checksum:           headChecksum(output),
		Metadata:           cloneMetadata(output.Metadata),
	}
}

func objectFromGet(key string, output *s3.GetObjectOutput) backend.Object {
	return backend.Object{
		Key:                key,
		Size:               totalSize(aws.ToString(output.ContentRange), aws.ToInt64(output.ContentLength)),
		ETag:               trimETag(aws.ToString(output.ETag)),
		Revision:           trimETag(aws.ToString(output.ETag)),
		LastModified:       aws.ToTime(output.LastModified),
		ContentType:        aws.ToString(output.ContentType),
		ContentEncoding:    aws.ToString(output.ContentEncoding),
		ContentDisposition: aws.ToString(output.ContentDisposition),
		CacheControl:       aws.ToString(output.CacheControl),
		VersionID:          aws.ToString(output.VersionId),
		Metadata:           cloneMetadata(output.Metadata),
	}
}

func byteRange(offset int64, length *int64) string {
	if length == nil {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+*length-1)
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

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return aws.String(value)
}

func trimETag(value string) string {
	return strings.Trim(value, "\"")
}

func optionalQuotedETag(value string) *string {
	value = strings.Trim(value, "\"")
	if value == "" {
		return nil
	}
	return aws.String("\"" + value + "\"")
}

func isNotFound(err error) bool {
	var responseError *smithyhttp.ResponseError
	return errors.As(err, &responseError) && responseError.HTTPStatusCode() == 404
}

func isConflict(err error) bool {
	var responseError *smithyhttp.ResponseError
	if !errors.As(err, &responseError) {
		return false
	}
	return responseError.HTTPStatusCode() == 409 || responseError.HTTPStatusCode() == 412
}

func isNotImplemented(err error) bool {
	var responseError *smithyhttp.ResponseError
	return errors.As(err, &responseError) && responseError.HTTPStatusCode() == 501
}

func cloneMetadata(value map[string]string) map[string]string {
	if len(value) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(value))
	for key, item := range value {
		cloned[key] = item
	}
	return cloned
}

func headChecksum(output *s3.HeadObjectOutput) string {
	switch {
	case output.ChecksumSHA256 != nil:
		return "SHA256:" + aws.ToString(output.ChecksumSHA256)
	case output.ChecksumSHA1 != nil:
		return "SHA1:" + aws.ToString(output.ChecksumSHA1)
	case output.ChecksumCRC64NVME != nil:
		return "CRC64NVME:" + aws.ToString(output.ChecksumCRC64NVME)
	case output.ChecksumCRC32C != nil:
		return "CRC32C:" + aws.ToString(output.ChecksumCRC32C)
	case output.ChecksumCRC32 != nil:
		return "CRC32:" + aws.ToString(output.ChecksumCRC32)
	default:
		return ""
	}
}

func uploadChecksum(output *transfermanager.UploadObjectOutput) string {
	switch {
	case output.ChecksumSHA256 != nil:
		return "SHA256:" + aws.ToString(output.ChecksumSHA256)
	case output.ChecksumSHA1 != nil:
		return "SHA1:" + aws.ToString(output.ChecksumSHA1)
	case output.ChecksumCRC64NVME != nil:
		return "CRC64NVME:" + aws.ToString(output.ChecksumCRC64NVME)
	case output.ChecksumCRC32C != nil:
		return "CRC32C:" + aws.ToString(output.ChecksumCRC32C)
	case output.ChecksumCRC32 != nil:
		return "CRC32:" + aws.ToString(output.ChecksumCRC32)
	default:
		return ""
	}
}

var _ backend.Store = (*Store)(nil)
