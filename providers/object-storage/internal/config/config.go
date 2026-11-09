// Package config loads and validates object-storage provider configuration.
package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultListenAddress       = ":50051"
	defaultShutdownGracePeriod = 10 * time.Second
	defaultMaxLobBytes         = 5 * 1024 * 1024 * 1024
	defaultMaxLobChunkBytes    = 256 * 1024
	defaultLobRetention        = 5 * time.Minute
	maxConfiguredLobChunkBytes = 4 * 1024 * 1024
)

var logicalNamePattern = regexp.MustCompile("^[A-Za-z_][A-Za-z0-9_]*$")
var environmentNamePattern = regexp.MustCompile("^[A-Za-z_][A-Za-z0-9_]*$")

// Config describes one provider process and its logical buckets.
type Config struct {
	ListenAddress       string
	LogLevel            slog.Level
	ShutdownGracePeriod time.Duration
	Lobs                Lobs
	Backends            map[string]Backend
	Buckets             map[string]Bucket
}

// Lobs controls provider-owned references returned for object content.
type Lobs struct {
	MaxBytes      uint64
	MaxChunkBytes uint32
	Retention     time.Duration
}

// Backend describes one reusable object-store connection.
type Backend struct {
	Type                    string
	Root                    string
	CreateDirectories       bool
	Region                  string
	Endpoint                string
	Profile                 string
	ForcePathStyle          bool
	UploadPartSizeBytes     int64
	MultipartThresholdBytes int64
	TransferConcurrency     int
	AccountURL              string
	Authentication          string
	AccountName             string
	AccountKeyEnvironment   string
	UploadBlockSizeBytes    int64
	UploadChunkSizeBytes    int64
}

// Bucket maps one public logical namespace to a backend-owned physical bucket.
type Bucket struct {
	Backend            string
	Name               string
	Prefix             string
	AllowUnboundedList bool
}

type fileConfig struct {
	ListenAddress       string                 `yaml:"listen"`
	LogLevel            string                 `yaml:"logLevel"`
	ShutdownGracePeriod string                 `yaml:"shutdownGracePeriod"`
	Lobs                fileLobs               `yaml:"lobs"`
	Backends            map[string]fileBackend `yaml:"backends"`
	Buckets             map[string]fileBucket  `yaml:"buckets"`
}

type fileLobs struct {
	MaxBytes      uint64 `yaml:"maxBytes"`
	MaxChunkBytes uint32 `yaml:"maxChunkBytes"`
	Retention     string `yaml:"retention"`
}

type fileBackend struct {
	Type                    string `yaml:"type"`
	Root                    string `yaml:"root"`
	CreateDirectories       bool   `yaml:"createDirectories"`
	Region                  string `yaml:"region"`
	Endpoint                string `yaml:"endpoint"`
	Profile                 string `yaml:"profile"`
	ForcePathStyle          bool   `yaml:"forcePathStyle"`
	UploadPartSizeBytes     int64  `yaml:"uploadPartSizeBytes"`
	MultipartThresholdBytes int64  `yaml:"multipartThresholdBytes"`
	TransferConcurrency     int    `yaml:"transferConcurrency"`
	AccountURL              string `yaml:"accountUrl"`
	Authentication          string `yaml:"authentication"`
	AccountName             string `yaml:"accountName"`
	AccountKeyEnvironment   string `yaml:"accountKeyEnvironment"`
	UploadBlockSizeBytes    int64  `yaml:"uploadBlockSizeBytes"`
	UploadChunkSizeBytes    int64  `yaml:"uploadChunkSizeBytes"`
}

type fileBucket struct {
	Backend            string `yaml:"backend"`
	Name               string `yaml:"name"`
	Prefix             string `yaml:"prefix"`
	AllowUnboundedList bool   `yaml:"allowUnboundedList"`
}

// Load reads one strict YAML configuration document.
func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open object-storage provider config: %w", err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)

	var serialized fileConfig
	if err := decoder.Decode(&serialized); err != nil {
		return Config{}, fmt.Errorf("decode object-storage provider config: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("decode object-storage provider config: multiple YAML documents are not supported")
		}
		return Config{}, fmt.Errorf("decode object-storage provider config: %w", err)
	}

	return serialized.normalize()
}

func (serialized fileConfig) normalize() (Config, error) {
	listenAddress := strings.TrimSpace(serialized.ListenAddress)
	if listenAddress == "" {
		listenAddress = defaultListenAddress
	}

	logLevel, err := parseLogLevel(serialized.LogLevel)
	if err != nil {
		return Config{}, err
	}
	gracePeriod := defaultShutdownGracePeriod
	if value := strings.TrimSpace(serialized.ShutdownGracePeriod); value != "" {
		gracePeriod, err = time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse shutdownGracePeriod: %w", err)
		}
		if gracePeriod <= 0 {
			return Config{}, errors.New("shutdownGracePeriod must be greater than zero")
		}
	}
	if len(serialized.Backends) == 0 {
		return Config{}, errors.New("at least one object-storage backend is required")
	}
	if len(serialized.Buckets) == 0 {
		return Config{}, errors.New("at least one logical bucket is required")
	}
	lobs, err := serialized.Lobs.normalize()
	if err != nil {
		return Config{}, err
	}

	backends := make(map[string]Backend, len(serialized.Backends))
	backendNames := sortedKeys(serialized.Backends)
	seenBackendNames := make(map[string]string, len(backendNames))
	for _, name := range backendNames {
		if err := validateLogicalName("backend", name); err != nil {
			return Config{}, err
		}
		folded := strings.ToUpper(name)
		if previous := seenBackendNames[folded]; previous != "" {
			return Config{}, fmt.Errorf("backend names %q and %q differ only by case", previous, name)
		}
		seenBackendNames[folded] = name

		backend, err := serialized.Backends[name].normalize()
		if err != nil {
			return Config{}, fmt.Errorf("backend %q: %w", name, err)
		}
		backends[name] = backend
	}

	buckets := make(map[string]Bucket, len(serialized.Buckets))
	bucketNames := sortedKeys(serialized.Buckets)
	seenBucketNames := make(map[string]string, len(bucketNames))
	for _, name := range bucketNames {
		if err := validateLogicalName("bucket", name); err != nil {
			return Config{}, err
		}
		folded := strings.ToUpper(name)
		if previous := seenBucketNames[folded]; previous != "" {
			return Config{}, fmt.Errorf("bucket names %q and %q differ only by case", previous, name)
		}
		seenBucketNames[folded] = name

		bucket, err := serialized.Buckets[name].normalize(backends)
		if err != nil {
			return Config{}, fmt.Errorf("bucket %q: %w", name, err)
		}
		buckets[name] = bucket
	}

	return Config{
		ListenAddress:       listenAddress,
		LogLevel:            logLevel,
		ShutdownGracePeriod: gracePeriod,
		Lobs:                lobs,
		Backends:            backends,
		Buckets:             buckets,
	}, nil
}

func (serialized fileLobs) normalize() (Lobs, error) {
	maxBytes := serialized.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultMaxLobBytes
	}
	maxChunkBytes := serialized.MaxChunkBytes
	if maxChunkBytes == 0 {
		maxChunkBytes = defaultMaxLobChunkBytes
	}
	retention := defaultLobRetention
	if value := strings.TrimSpace(serialized.Retention); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return Lobs{}, fmt.Errorf("parse lobs.retention: %w", err)
		}
		retention = parsed
	}
	if maxBytes > uint64(math.MaxInt64) {
		return Lobs{}, errors.New("lobs.maxBytes exceeds the supported object size")
	}
	if maxChunkBytes > maxConfiguredLobChunkBytes {
		return Lobs{}, fmt.Errorf(
			"lobs.maxChunkBytes must not exceed %d bytes",
			maxConfiguredLobChunkBytes,
		)
	}
	if retention < time.Second {
		return Lobs{}, errors.New("lobs.retention must be at least one second")
	}
	return Lobs{
		MaxBytes:      maxBytes,
		MaxChunkBytes: maxChunkBytes,
		Retention:     retention,
	}, nil
}

func (serialized fileBackend) normalize() (Backend, error) {
	backendType := strings.ToLower(strings.TrimSpace(serialized.Type))
	if backendType == "" {
		return Backend{}, errors.New("type is required")
	}
	switch backendType {
	case "s3":
		return serialized.normalizeS3()
	case "azure-blob":
		return serialized.normalizeAzureBlob()
	case "gcs":
		return serialized.normalizeGCS()
	case "filesystem":
		return serialized.normalizeFilesystem()
	default:
		return Backend{}, fmt.Errorf("unsupported type %q", serialized.Type)
	}
}

func (serialized fileBackend) normalizeS3() (Backend, error) {
	if strings.TrimSpace(serialized.Root) != "" || serialized.CreateDirectories ||
		strings.TrimSpace(serialized.AccountURL) != "" ||
		strings.TrimSpace(serialized.Authentication) != "" ||
		strings.TrimSpace(serialized.AccountName) != "" ||
		strings.TrimSpace(serialized.AccountKeyEnvironment) != "" ||
		serialized.UploadBlockSizeBytes != 0 ||
		serialized.UploadChunkSizeBytes != 0 {
		return Backend{}, errors.New("filesystem, Azure and GCS settings are not valid for S3 backends")
	}
	region := strings.TrimSpace(serialized.Region)
	if region == "" {
		return Backend{}, errors.New("region is required")
	}
	endpoint := strings.TrimSpace(serialized.Endpoint)
	if endpoint != "" {
		parsed, err := url.Parse(endpoint)
		if err != nil {
			return Backend{}, fmt.Errorf("parse endpoint: %w", err)
		}
		if (parsed.Scheme != "http" && parsed.Scheme != "https") ||
			parsed.Host == "" ||
			parsed.User != nil ||
			parsed.RawQuery != "" ||
			parsed.Fragment != "" {
			return Backend{}, errors.New("endpoint must be an absolute HTTP(S) URL without credentials, query or fragment")
		}
		endpoint = strings.TrimRight(endpoint, "/")
	}
	if serialized.UploadPartSizeBytes != 0 &&
		serialized.UploadPartSizeBytes < 5*1024*1024 {
		return Backend{}, errors.New("uploadPartSizeBytes must be at least 5 MiB")
	}
	if serialized.MultipartThresholdBytes < 0 {
		return Backend{}, errors.New("multipartThresholdBytes must not be negative")
	}
	if serialized.MultipartThresholdBytes != 0 &&
		serialized.UploadPartSizeBytes != 0 &&
		serialized.MultipartThresholdBytes < serialized.UploadPartSizeBytes {
		return Backend{}, errors.New("multipartThresholdBytes must be at least uploadPartSizeBytes")
	}
	if serialized.TransferConcurrency < 0 {
		return Backend{}, errors.New("transferConcurrency must not be negative")
	}

	return Backend{
		Type:                    "s3",
		Region:                  region,
		Endpoint:                endpoint,
		Profile:                 strings.TrimSpace(serialized.Profile),
		ForcePathStyle:          serialized.ForcePathStyle,
		UploadPartSizeBytes:     serialized.UploadPartSizeBytes,
		MultipartThresholdBytes: serialized.MultipartThresholdBytes,
		TransferConcurrency:     serialized.TransferConcurrency,
	}, nil
}

func (serialized fileBackend) normalizeAzureBlob() (Backend, error) {
	if strings.TrimSpace(serialized.Root) != "" || serialized.CreateDirectories ||
		strings.TrimSpace(serialized.Region) != "" ||
		strings.TrimSpace(serialized.Endpoint) != "" ||
		strings.TrimSpace(serialized.Profile) != "" ||
		serialized.ForcePathStyle ||
		serialized.UploadPartSizeBytes != 0 ||
		serialized.MultipartThresholdBytes != 0 ||
		serialized.UploadChunkSizeBytes != 0 {
		return Backend{}, errors.New("filesystem, S3 and GCS settings are not valid for Azure Blob backends")
	}

	accountURL, err := normalizeServiceURL(serialized.AccountURL)
	if err != nil {
		return Backend{}, fmt.Errorf("accountUrl: %w", err)
	}
	authentication := strings.ToLower(strings.TrimSpace(serialized.Authentication))
	if authentication == "" {
		authentication = "default"
	}
	accountName := strings.TrimSpace(serialized.AccountName)
	accountKeyEnvironment := strings.TrimSpace(serialized.AccountKeyEnvironment)
	switch authentication {
	case "default":
		if accountName != "" || accountKeyEnvironment != "" {
			return Backend{}, errors.New("accountName and accountKeyEnvironment require shared-key authentication")
		}
		if !strings.HasPrefix(accountURL, "https://") {
			return Backend{}, errors.New("default authentication requires an HTTPS accountUrl")
		}
	case "shared-key":
		if accountName == "" {
			return Backend{}, errors.New("accountName is required for shared-key authentication")
		}
		if accountKeyEnvironment == "" {
			return Backend{}, errors.New("accountKeyEnvironment is required for shared-key authentication")
		}
		if !environmentNamePattern.MatchString(accountKeyEnvironment) {
			return Backend{}, errors.New("accountKeyEnvironment must be a valid environment variable name")
		}
	default:
		return Backend{}, fmt.Errorf("unsupported authentication %q", serialized.Authentication)
	}
	if serialized.UploadBlockSizeBytes != 0 && serialized.UploadBlockSizeBytes < 1024*1024 {
		return Backend{}, errors.New("uploadBlockSizeBytes must be at least 1 MiB")
	}
	if serialized.TransferConcurrency < 0 {
		return Backend{}, errors.New("transferConcurrency must not be negative")
	}

	return Backend{
		Type:                  "azure-blob",
		AccountURL:            accountURL,
		Authentication:        authentication,
		AccountName:           accountName,
		AccountKeyEnvironment: accountKeyEnvironment,
		UploadBlockSizeBytes:  serialized.UploadBlockSizeBytes,
		TransferConcurrency:   serialized.TransferConcurrency,
	}, nil
}

func (serialized fileBackend) normalizeGCS() (Backend, error) {
	if strings.TrimSpace(serialized.Root) != "" || serialized.CreateDirectories ||
		strings.TrimSpace(serialized.Region) != "" ||
		strings.TrimSpace(serialized.Profile) != "" ||
		serialized.ForcePathStyle ||
		serialized.UploadPartSizeBytes != 0 ||
		serialized.MultipartThresholdBytes != 0 ||
		serialized.TransferConcurrency != 0 ||
		strings.TrimSpace(serialized.AccountURL) != "" ||
		strings.TrimSpace(serialized.AccountName) != "" ||
		strings.TrimSpace(serialized.AccountKeyEnvironment) != "" ||
		serialized.UploadBlockSizeBytes != 0 {
		return Backend{}, errors.New("filesystem, S3 and Azure settings are not valid for GCS backends")
	}

	authentication := strings.ToLower(strings.TrimSpace(serialized.Authentication))
	if authentication == "" {
		authentication = "default"
	}
	endpoint := strings.TrimSpace(serialized.Endpoint)
	switch authentication {
	case "default":
		if endpoint != "" {
			return Backend{}, errors.New("endpoint requires emulator authentication")
		}
	case "emulator":
		var err error
		endpoint, err = normalizeServiceURL(endpoint)
		if err != nil {
			return Backend{}, fmt.Errorf("endpoint: %w", err)
		}
	default:
		return Backend{}, fmt.Errorf("unsupported authentication %q", serialized.Authentication)
	}
	if serialized.UploadChunkSizeBytes != 0 && serialized.UploadChunkSizeBytes < 256*1024 {
		return Backend{}, errors.New("uploadChunkSizeBytes must be at least 256 KiB")
	}
	maxInt := int64(^uint(0) >> 1)
	if serialized.UploadChunkSizeBytes > maxInt {
		return Backend{}, errors.New("uploadChunkSizeBytes exceeds the platform integer size")
	}

	return Backend{
		Type:                 "gcs",
		Endpoint:             endpoint,
		Authentication:       authentication,
		UploadChunkSizeBytes: serialized.UploadChunkSizeBytes,
	}, nil
}

func normalizeServiceURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("is required")
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("parse URL: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("must be an absolute HTTP(S) URL without credentials, query or fragment")
	}
	return strings.TrimRight(value, "/"), nil
}

func (serialized fileBackend) normalizeFilesystem() (Backend, error) {
	if strings.TrimSpace(serialized.Region) != "" ||
		strings.TrimSpace(serialized.Endpoint) != "" ||
		strings.TrimSpace(serialized.Profile) != "" ||
		serialized.ForcePathStyle ||
		serialized.UploadPartSizeBytes != 0 ||
		serialized.MultipartThresholdBytes != 0 ||
		serialized.TransferConcurrency != 0 ||
		strings.TrimSpace(serialized.AccountURL) != "" ||
		strings.TrimSpace(serialized.Authentication) != "" ||
		strings.TrimSpace(serialized.AccountName) != "" ||
		strings.TrimSpace(serialized.AccountKeyEnvironment) != "" ||
		serialized.UploadBlockSizeBytes != 0 ||
		serialized.UploadChunkSizeBytes != 0 {
		return Backend{}, errors.New("cloud connection and transfer settings are not valid for filesystem backends")
	}

	root := strings.TrimSpace(serialized.Root)
	if root == "" {
		return Backend{}, errors.New("root is required")
	}
	if !filepath.IsAbs(root) {
		return Backend{}, errors.New("root must be an absolute path")
	}
	root = filepath.Clean(root)
	if root == string(filepath.Separator) {
		return Backend{}, errors.New("root must not be the filesystem root")
	}

	return Backend{
		Type:              "filesystem",
		Root:              root,
		CreateDirectories: serialized.CreateDirectories,
	}, nil
}

func (serialized fileBucket) normalize(backends map[string]Backend) (Bucket, error) {
	backendName := strings.TrimSpace(serialized.Backend)
	if backendName == "" {
		return Bucket{}, errors.New("backend is required")
	}
	backend, exists := backends[backendName]
	if !exists {
		return Bucket{}, fmt.Errorf("backend %q is not configured", backendName)
	}
	physicalName := strings.TrimSpace(serialized.Name)
	if physicalName == "" {
		return Bucket{}, errors.New("name is required")
	}
	if strings.ContainsRune(physicalName, '\x00') {
		return Bucket{}, errors.New("name contains a NUL byte")
	}
	if backend.Type == "filesystem" {
		if physicalName == "." || physicalName == ".." ||
			strings.ContainsAny(physicalName, "/\\") {
			return Bucket{}, errors.New("filesystem bucket name must be one safe directory name")
		}
		if strings.HasPrefix(physicalName, ".kubling-object-storage") {
			return Bucket{}, errors.New("filesystem bucket name uses a reserved provider prefix")
		}
	}

	prefix, err := normalizePrefix(serialized.Prefix)
	if err != nil {
		return Bucket{}, err
	}
	return Bucket{
		Backend:            backendName,
		Name:               physicalName,
		Prefix:             prefix,
		AllowUnboundedList: serialized.AllowUnboundedList,
	}, nil
}

func normalizePrefix(value string) (string, error) {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "/")
	if value == "" {
		return "", nil
	}
	if strings.ContainsRune(value, '\x00') || strings.Contains(value, "\\") {
		return "", errors.New("prefix contains an unsafe character")
	}
	for _, segment := range strings.Split(value, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", errors.New("prefix contains an unsafe path segment")
		}
	}
	return value + "/", nil
}

func validateLogicalName(kind string, name string) error {
	if !logicalNamePattern.MatchString(name) {
		return fmt.Errorf("%s name %q must match %s", kind, name, logicalNamePattern)
	}
	return nil
}

func parseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unsupported logLevel %q", value)
	}
}

func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
