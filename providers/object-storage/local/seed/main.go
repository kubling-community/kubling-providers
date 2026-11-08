package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"google.golang.org/api/option"
)

func main() {
	backendType := flag.String("backend", "s3", "backend type: s3, azure-blob or gcs")
	endpoint := flag.String("endpoint", "http://127.0.0.1:9000", "object-store endpoint")
	accountName := flag.String("account-name", "", "Azure storage account name")
	projectID := flag.String("project-id", "kubling-local", "GCS project identifier")
	flag.Parse()
	var err error
	switch *backendType {
	case "s3":
		err = seedS3(context.Background(), *endpoint)
	case "azure-blob":
		err = seedAzure(context.Background(), *endpoint, *accountName, os.Getenv("AZURE_STORAGE_ACCOUNT_KEY"))
	case "gcs":
		err = seedGCS(context.Background(), *endpoint, *projectID)
	default:
		err = fmt.Errorf("unsupported backend %q", *backendType)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func seedGCS(ctx context.Context, endpoint string, projectID string) error {
	client, err := storage.NewClient(
		ctx,
		option.WithEndpoint(strings.TrimRight(endpoint, "/")+"/storage/v1/"),
		option.WithoutAuthentication(),
		storage.WithJSONReads(),
		storage.WithDisabledClientMetrics(),
	)
	if err != nil {
		return fmt.Errorf("create local GCS client: %w", err)
	}
	defer client.Close()

	for _, bucketName := range []string{"kubling-media", "kubling-archive"} {
		if err := ensureGCSBucket(ctx, client, projectID, bucketName); err != nil {
			return err
		}
	}
	fixtures := []struct {
		key         string
		contentType string
		content     []byte
	}{
		{key: "objects/readme.txt", contentType: "text/plain", content: []byte("Kubling object storage fixture\n")},
		{key: "objects/events/sample.json", contentType: "application/json", content: []byte(`{"source":"fixture","status":"ready"}`)},
	}
	for _, fixture := range fixtures {
		writer := client.Bucket("kubling-media").Object(fixture.key).NewWriter(ctx)
		writer.ContentType = fixture.contentType
		writer.Metadata = map[string]string{"fixture": "object-storage"}
		if _, err := io.Copy(writer, bytes.NewReader(fixture.content)); err != nil {
			return fmt.Errorf("write GCS fixture %q: %w", fixture.key, err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("close GCS fixture %q: %w", fixture.key, err)
		}
	}
	return nil
}

func seedS3(ctx context.Context, endpoint string) error {
	configuration, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"kubling-local",
			"kubling-local-secret",
			"",
		)),
	)
	if err != nil {
		return fmt.Errorf("load local S3 configuration: %w", err)
	}
	client := s3.NewFromConfig(configuration, func(options *s3.Options) {
		options.BaseEndpoint = aws.String(endpoint)
		options.UsePathStyle = true
	})
	for _, bucket := range []string{"kubling-media", "kubling-archive"} {
		if err := ensureBucket(ctx, client, bucket); err != nil {
			return err
		}
	}
	fixtures := []struct {
		key         string
		contentType string
		content     []byte
	}{
		{key: "objects/readme.txt", contentType: "text/plain", content: []byte("Kubling object storage fixture\n")},
		{key: "objects/events/sample.json", contentType: "application/json", content: []byte(`{"source":"fixture","status":"ready"}`)},
	}
	for _, fixture := range fixtures {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String("kubling-media"),
			Key:         aws.String(fixture.key),
			Body:        bytes.NewReader(fixture.content),
			ContentType: aws.String(fixture.contentType),
			Metadata:    map[string]string{"fixture": "object-storage"},
		})
		if err != nil {
			return fmt.Errorf("seed object %q: %w", fixture.key, err)
		}
	}
	return nil
}

func seedAzure(ctx context.Context, endpoint string, accountName string, accountKey string) error {
	if accountName == "" || accountKey == "" {
		return fmt.Errorf("Azure account name and AZURE_STORAGE_ACCOUNT_KEY are required")
	}
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return fmt.Errorf("create local Azure credential: %w", err)
	}
	client, err := azblob.NewClientWithSharedKeyCredential(endpoint, credential, nil)
	if err != nil {
		return fmt.Errorf("create local Azure client: %w", err)
	}
	for _, containerName := range []string{"kubling-media", "kubling-archive"} {
		if err := ensureAzureContainer(ctx, client, containerName); err != nil {
			return err
		}
	}
	fixtures := []struct {
		key         string
		contentType string
		content     []byte
	}{
		{key: "objects/readme.txt", contentType: "text/plain", content: []byte("Kubling object storage fixture\n")},
		{key: "objects/events/sample.json", contentType: "application/json", content: []byte(`{"source":"fixture","status":"ready"}`)},
	}
	metadataValue := "object-storage"
	for _, fixture := range fixtures {
		_, err := client.UploadBuffer(
			ctx,
			"kubling-media",
			fixture.key,
			fixture.content,
			&azblob.UploadBufferOptions{
				HTTPHeaders: &blob.HTTPHeaders{BlobContentType: &fixture.contentType},
				Metadata:    map[string]*string{"fixture": &metadataValue},
			},
		)
		if err != nil {
			return fmt.Errorf("seed Azure blob %q: %w", fixture.key, err)
		}
	}
	return nil
}

func ensureBucket(ctx context.Context, client *s3.Client, name string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(name)})
		if err == nil {
			return nil
		}
		_, createErr := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(name)})
		if createErr == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("create local S3 bucket %q: %w", name, createErr)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func ensureAzureContainer(ctx context.Context, client *azblob.Client, name string) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		containerClient := client.ServiceClient().NewContainerClient(name)
		if _, err := containerClient.GetProperties(ctx, nil); err == nil {
			return nil
		}
		if _, err := client.CreateContainer(ctx, name, nil); err == nil {
			return nil
		} else if time.Now().After(deadline) {
			return fmt.Errorf("create local Azure container %q: %w", name, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func ensureGCSBucket(
	ctx context.Context,
	client *storage.Client,
	projectID string,
	name string,
) error {
	deadline := time.Now().Add(30 * time.Second)
	bucket := client.Bucket(name)
	for {
		if _, err := bucket.Attrs(ctx); err == nil {
			return nil
		} else if !errors.Is(err, storage.ErrBucketNotExist) && time.Now().After(deadline) {
			return fmt.Errorf("inspect local GCS bucket %q: %w", name, err)
		}
		if err := bucket.Create(ctx, projectID, nil); err == nil {
			return nil
		} else if time.Now().After(deadline) {
			return fmt.Errorf("create local GCS bucket %q: %w", name, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}
