package s3store

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
)

func TestListScopesRequestAndReturnsLogicalKeys(t *testing.T) {
	var captured *s3.ListObjectsV2Input
	modified := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	client := &fakeS3{
		listObjects: func(input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
			captured = input
			return &s3.ListObjectsV2Output{
				Contents: []s3types.Object{
					{
						Key:          aws.String("tenant/acme/images/logo.png"),
						Size:         aws.Int64(42),
						ETag:         aws.String("\"abc\""),
						LastModified: &modified,
					},
					{Key: aws.String("another/root/ignored")},
				},
				NextContinuationToken: aws.String("next"),
			}, nil
		},
	}
	store := newWithClients(client, &fakeUploader{})
	page, err := store.List(
		context.Background(),
		backend.Bucket{Name: "physical", Prefix: "tenant/acme/"},
		backend.ListRequest{Prefix: "images/", Limit: 100},
	)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if got := aws.ToString(captured.Bucket); got != "physical" {
		t.Fatalf("bucket = %q", got)
	}
	if got := aws.ToString(captured.Prefix); got != "tenant/acme/images/" {
		t.Fatalf("prefix = %q", got)
	}
	if got := aws.ToInt32(captured.MaxKeys); got != 100 {
		t.Fatalf("max keys = %d", got)
	}
	if len(page.Objects) != 1 || page.Objects[0].Key != "images/logo.png" {
		t.Fatalf("objects = %+v", page.Objects)
	}
	if page.Objects[0].ETag != "abc" || page.Objects[0].Size != 42 {
		t.Fatalf("object metadata = %+v", page.Objects[0])
	}
	if page.ContinuationToken != "next" {
		t.Fatalf("continuation token = %q", page.ContinuationToken)
	}
	wantFields := backend.ObjectFieldKey |
		backend.ObjectFieldContent |
		backend.ObjectFieldContentLength |
		backend.ObjectFieldETag |
		backend.ObjectFieldLastModified
	if page.Fields != wantFields {
		t.Fatalf("list fields = %v, want %v", page.Fields, wantFields)
	}
}

func TestOpenBuildsRangeAndPreservesTotalSize(t *testing.T) {
	var captured *s3.GetObjectInput
	client := &fakeS3{
		getObject: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			captured = input
			return &s3.GetObjectOutput{
				Body:          io.NopCloser(bytes.NewReader([]byte("567"))),
				ContentLength: aws.Int64(3),
				ContentRange:  aws.String("bytes 5-7/10"),
			}, nil
		},
	}
	store := newWithClients(client, &fakeUploader{})
	length := int64(3)
	reader, err := store.Open(
		context.Background(),
		backend.Bucket{Name: "physical", Prefix: "root/"},
		backend.GetRequest{
			Key: "video.bin", Offset: 5, Length: &length,
			VersionID: "version-1", Revision: "etag-1",
		},
	)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer reader.Body.Close()
	if got := aws.ToString(captured.Key); got != "root/video.bin" {
		t.Fatalf("key = %q", got)
	}
	if got := aws.ToString(captured.Range); got != "bytes=5-7" {
		t.Fatalf("range = %q", got)
	}
	if got := aws.ToString(captured.VersionId); got != "version-1" {
		t.Fatalf("version id = %q", got)
	}
	if got := aws.ToString(captured.IfMatch); got != "\"etag-1\"" {
		t.Fatalf("if-match = %q", got)
	}
	if reader.Object.Size != 10 {
		t.Fatalf("total size = %d", reader.Object.Size)
	}
	data, err := io.ReadAll(reader.Body)
	if err != nil || string(data) != "567" {
		t.Fatalf("read = %q, %v", data, err)
	}
}

func TestPutUsesTransferManagerAndPortableMetadata(t *testing.T) {
	var captured *transfermanager.UploadObjectInput
	uploader := &fakeUploader{
		upload: func(input *transfermanager.UploadObjectInput) (*transfermanager.UploadObjectOutput, error) {
			captured = input
			return &transfermanager.UploadObjectOutput{
				ETag:      aws.String("\"etag\""),
				VersionID: aws.String("version-1"),
			}, nil
		},
	}
	store := newWithClients(&fakeS3{}, uploader)
	size := int64(7)
	result, err := store.Put(
		context.Background(),
		backend.Bucket{Name: "physical", Prefix: "root/"},
		backend.PutRequest{
			Key:         "images/a.bin",
			Body:        bytes.NewReader([]byte("content")),
			Size:        &size,
			ContentType: "application/octet-stream",
			Metadata:    map[string]string{"source": "test"},
			IfNoneMatch: true,
		},
	)
	if err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if got := aws.ToString(captured.Key); got != "root/images/a.bin" {
		t.Fatalf("key = %q", got)
	}
	if got := aws.ToString(captured.ContentType); got != "application/octet-stream" {
		t.Fatalf("content type = %q", got)
	}
	if got := captured.Metadata["source"]; got != "test" {
		t.Fatalf("metadata source = %q", got)
	}
	if got := aws.ToString(captured.IfNoneMatch); got != "*" {
		t.Fatalf("if-none-match = %q", got)
	}
	if result.Key != "images/a.bin" || result.Size != 7 ||
		result.ETag != "etag" || result.VersionID != "version-1" {
		t.Fatalf("result = %+v", result)
	}
}

func TestZeroLengthOpenUsesHeadWithoutGet(t *testing.T) {
	getCalls := 0
	client := &fakeS3{
		headObject: func(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return &s3.HeadObjectOutput{ContentLength: aws.Int64(9)}, nil
		},
		getObject: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			getCalls++
			return nil, nil
		},
	}
	store := newWithClients(client, &fakeUploader{})
	zero := int64(0)
	reader, err := store.Open(
		context.Background(),
		backend.Bucket{Name: "physical"},
		backend.GetRequest{Key: "empty-range", Length: &zero},
	)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer reader.Body.Close()
	if getCalls != 0 || reader.Object.Size != 9 {
		t.Fatalf("get calls/size = %d/%d", getCalls, reader.Object.Size)
	}
}

func TestDeleteUsesVersionAndRevisionConditions(t *testing.T) {
	var captured *s3.DeleteObjectInput
	store := newWithClients(&fakeS3{
		deleteObject: func(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
			captured = input
			return &s3.DeleteObjectOutput{}, nil
		},
	}, &fakeUploader{})
	err := store.Delete(
		context.Background(),
		backend.Bucket{Name: "physical", Prefix: "root/"},
		backend.DeleteRequest{
			Key: "images/a.bin", VersionID: "version-1", IfRevisionMatch: "etag-1",
		},
	)
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if got := aws.ToString(captured.Key); got != "root/images/a.bin" {
		t.Fatalf("key = %q", got)
	}
	if got := aws.ToString(captured.VersionId); got != "version-1" {
		t.Fatalf("version id = %q", got)
	}
	if got := aws.ToString(captured.IfMatch); got != `"etag-1"` {
		t.Fatalf("if-match = %q", got)
	}
}

func TestUpdateAttributesUsesConditionalSelfCopy(t *testing.T) {
	var captured *s3.CopyObjectInput
	store := newWithClients(&fakeS3{
		headObject: func(input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return &s3.HeadObjectOutput{
				ContentLength:        aws.Int64(7),
				ContentType:          aws.String("image/png"),
				ContentLanguage:      aws.String("en"),
				StorageClass:         s3types.StorageClassStandardIa,
				ServerSideEncryption: s3types.ServerSideEncryptionAes256,
			}, nil
		},
		getObjectACL: func(input *s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error) {
			return &s3.GetObjectAclOutput{
				Owner: &s3types.Owner{ID: aws.String("owner")},
				Grants: []s3types.Grant{
					{
						Grantee: &s3types.Grantee{
							Type: s3types.TypeCanonicalUser,
							ID:   aws.String("owner"),
						},
						Permission: s3types.PermissionFullControl,
					},
					{
						Grantee: &s3types.Grantee{
							Type: s3types.TypeGroup,
							URI:  aws.String("https://acs.amazonaws.com/groups/global/AllUsers"),
						},
						Permission: s3types.PermissionRead,
					},
				},
			}, nil
		},
		copyObject: func(input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
			captured = input
			return &s3.CopyObjectOutput{}, nil
		},
	}, &fakeUploader{})
	err := store.UpdateAttributes(
		context.Background(),
		backend.Bucket{Name: "physical", Prefix: "root/"},
		backend.UpdateAttributesRequest{
			Key:             "images/a b.bin",
			Size:            7,
			ContentType:     "application/octet-stream",
			CacheControl:    "public, max-age=60",
			Metadata:        map[string]string{"stage": "reviewed"},
			Fields:          backend.AttributeFields{CacheControl: true, Metadata: true},
			IfRevisionMatch: "etag-1",
		},
	)
	if err != nil {
		t.Fatalf("UpdateAttributes() error = %v", err)
	}
	if got := aws.ToString(captured.Key); got != "root/images/a b.bin" {
		t.Fatalf("key = %q", got)
	}
	if got := aws.ToString(captured.CopySource); got != "physical%2Froot%2Fimages%2Fa%20b.bin" {
		t.Fatalf("copy source = %q", got)
	}
	if aws.ToString(captured.CopySourceIfMatch) != `"etag-1"` ||
		aws.ToString(captured.IfMatch) != `"etag-1"` ||
		captured.MetadataDirective != s3types.MetadataDirectiveReplace ||
		captured.Metadata["stage"] != "reviewed" ||
		aws.ToString(captured.CacheControl) != "public, max-age=60" ||
		aws.ToString(captured.ContentType) != "image/png" ||
		aws.ToString(captured.ContentLanguage) != "en" ||
		captured.StorageClass != s3types.StorageClassStandardIa ||
		captured.ServerSideEncryption != s3types.ServerSideEncryptionAes256 ||
		aws.ToString(captured.GrantFullControl) != `id="owner"` ||
		aws.ToString(captured.GrantRead) !=
			`uri="https://acs.amazonaws.com/groups/global/AllUsers"` {
		t.Fatalf("copy input = %+v", captured)
	}
}

func TestMultipartAttributeCopyUsesRangesAndConditionalCompletion(t *testing.T) {
	var ranges []string
	var completed *s3.CompleteMultipartUploadInput
	abortCalls := 0
	client := &fakeS3{
		createMultipartUpload: func(input *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
			if input.Metadata["stage"] != "reviewed" ||
				aws.ToString(input.Tagging) != "environment=test" {
				t.Fatalf("create metadata/tagging = %v/%q", input.Metadata, aws.ToString(input.Tagging))
			}
			return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload-1")}, nil
		},
		uploadPartCopy: func(input *s3.UploadPartCopyInput) (*s3.UploadPartCopyOutput, error) {
			ranges = append(ranges, aws.ToString(input.CopySourceRange))
			return &s3.UploadPartCopyOutput{CopyPartResult: &s3types.CopyPartResult{
				ETag: aws.String("part-etag"),
			}}, nil
		},
		completeMultipartUpload: func(input *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
			completed = input
			return &s3.CompleteMultipartUploadOutput{}, nil
		},
		abortMultipartUpload: func(input *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
			abortCalls++
			return &s3.AbortMultipartUploadOutput{}, nil
		},
	}
	store := newWithClients(client, &fakeUploader{})
	store.copyPartSize = minimumCopyPartSize
	size := int64(11 * 1024 * 1024)
	err := store.updateAttributesMultipart(
		context.Background(),
		"physical",
		"root/large.bin",
		"physical%2Froot%2Flarge.bin",
		backend.UpdateAttributesRequest{
			Key:             "large.bin",
			Size:            size,
			Metadata:        map[string]string{"stage": "reviewed"},
			IfRevisionMatch: "etag-1",
		},
		copyAttributes{
			metadata: map[string]string{"stage": "reviewed"},
			tagging:  aws.String("environment=test"),
		},
	)
	if err != nil {
		t.Fatalf("updateAttributesMultipart() error = %v", err)
	}
	wantRanges := []string{"bytes=0-5242879", "bytes=5242880-10485759", "bytes=10485760-11534335"}
	if len(ranges) != len(wantRanges) {
		t.Fatalf("ranges = %v", ranges)
	}
	for index := range wantRanges {
		if ranges[index] != wantRanges[index] {
			t.Fatalf("ranges = %v", ranges)
		}
	}
	if completed == nil || aws.ToString(completed.IfMatch) != `"etag-1"` ||
		len(completed.MultipartUpload.Parts) != 3 || abortCalls != 0 {
		t.Fatalf("completion=%+v aborts=%d", completed, abortCalls)
	}
}

type fakeS3 struct {
	headBucket              func(*s3.HeadBucketInput) (*s3.HeadBucketOutput, error)
	listObjects             func(*s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
	headObject              func(*s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	getObject               func(*s3.GetObjectInput) (*s3.GetObjectOutput, error)
	getObjectACL            func(*s3.GetObjectAclInput) (*s3.GetObjectAclOutput, error)
	getObjectTagging        func(*s3.GetObjectTaggingInput) (*s3.GetObjectTaggingOutput, error)
	copyObject              func(*s3.CopyObjectInput) (*s3.CopyObjectOutput, error)
	createMultipartUpload   func(*s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error)
	uploadPartCopy          func(*s3.UploadPartCopyInput) (*s3.UploadPartCopyOutput, error)
	completeMultipartUpload func(*s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error)
	abortMultipartUpload    func(*s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error)
	deleteObject            func(*s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
}

func (f *fakeS3) HeadBucket(
	_ context.Context,
	input *s3.HeadBucketInput,
	_ ...func(*s3.Options),
) (*s3.HeadBucketOutput, error) {
	if f.headBucket == nil {
		return &s3.HeadBucketOutput{}, nil
	}
	return f.headBucket(input)
}

func (f *fakeS3) ListObjectsV2(
	_ context.Context,
	input *s3.ListObjectsV2Input,
	_ ...func(*s3.Options),
) (*s3.ListObjectsV2Output, error) {
	return f.listObjects(input)
}

func (f *fakeS3) HeadObject(
	_ context.Context,
	input *s3.HeadObjectInput,
	_ ...func(*s3.Options),
) (*s3.HeadObjectOutput, error) {
	if f.headObject == nil {
		return &s3.HeadObjectOutput{}, nil
	}
	return f.headObject(input)
}

func (f *fakeS3) GetObject(
	_ context.Context,
	input *s3.GetObjectInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	if f.getObject == nil {
		return &s3.GetObjectOutput{}, nil
	}
	return f.getObject(input)
}

func (f *fakeS3) GetObjectAcl(
	_ context.Context,
	input *s3.GetObjectAclInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectAclOutput, error) {
	if f.getObjectACL == nil {
		return &s3.GetObjectAclOutput{}, nil
	}
	return f.getObjectACL(input)
}

func (f *fakeS3) GetObjectTagging(
	_ context.Context,
	input *s3.GetObjectTaggingInput,
	_ ...func(*s3.Options),
) (*s3.GetObjectTaggingOutput, error) {
	if f.getObjectTagging == nil {
		return &s3.GetObjectTaggingOutput{}, nil
	}
	return f.getObjectTagging(input)
}

func (f *fakeS3) CopyObject(
	_ context.Context,
	input *s3.CopyObjectInput,
	_ ...func(*s3.Options),
) (*s3.CopyObjectOutput, error) {
	if f.copyObject == nil {
		return &s3.CopyObjectOutput{}, nil
	}
	return f.copyObject(input)
}

func (f *fakeS3) CreateMultipartUpload(
	_ context.Context,
	input *s3.CreateMultipartUploadInput,
	_ ...func(*s3.Options),
) (*s3.CreateMultipartUploadOutput, error) {
	if f.createMultipartUpload == nil {
		return &s3.CreateMultipartUploadOutput{UploadId: aws.String("upload")}, nil
	}
	return f.createMultipartUpload(input)
}

func (f *fakeS3) UploadPartCopy(
	_ context.Context,
	input *s3.UploadPartCopyInput,
	_ ...func(*s3.Options),
) (*s3.UploadPartCopyOutput, error) {
	if f.uploadPartCopy == nil {
		return &s3.UploadPartCopyOutput{CopyPartResult: &s3types.CopyPartResult{
			ETag: aws.String("part"),
		}}, nil
	}
	return f.uploadPartCopy(input)
}

func (f *fakeS3) CompleteMultipartUpload(
	_ context.Context,
	input *s3.CompleteMultipartUploadInput,
	_ ...func(*s3.Options),
) (*s3.CompleteMultipartUploadOutput, error) {
	if f.completeMultipartUpload == nil {
		return &s3.CompleteMultipartUploadOutput{}, nil
	}
	return f.completeMultipartUpload(input)
}

func (f *fakeS3) AbortMultipartUpload(
	_ context.Context,
	input *s3.AbortMultipartUploadInput,
	_ ...func(*s3.Options),
) (*s3.AbortMultipartUploadOutput, error) {
	if f.abortMultipartUpload == nil {
		return &s3.AbortMultipartUploadOutput{}, nil
	}
	return f.abortMultipartUpload(input)
}

func (f *fakeS3) DeleteObject(
	_ context.Context,
	input *s3.DeleteObjectInput,
	_ ...func(*s3.Options),
) (*s3.DeleteObjectOutput, error) {
	if f.deleteObject == nil {
		return &s3.DeleteObjectOutput{}, nil
	}
	return f.deleteObject(input)
}

type fakeUploader struct {
	upload func(*transfermanager.UploadObjectInput) (*transfermanager.UploadObjectOutput, error)
}

func (f *fakeUploader) UploadObject(
	_ context.Context,
	input *transfermanager.UploadObjectInput,
	_ ...func(*transfermanager.Options),
) (*transfermanager.UploadObjectOutput, error) {
	if f.upload == nil {
		return &transfermanager.UploadObjectOutput{}, nil
	}
	return f.upload(input)
}
