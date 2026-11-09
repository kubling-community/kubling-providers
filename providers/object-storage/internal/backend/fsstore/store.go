// Package fsstore implements object storage on a confined local filesystem.
package fsstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	providerconfig "github.com/kubling-community/kubling-providers/providers/object-storage/internal/config"
)

const (
	internalDirectory = ".kubling-object-storage"
	temporaryPrefix   = ".kubling-object-storage-tmp-"
	defaultListLimit  = int32(1000)
)

var errClosed = errors.New("filesystem object store is closed")

// Store is a filesystem backend rooted at one configured directory.
//
// All paths are resolved through os.Root so object keys and symlinks cannot
// escape the configured root. Mutations are serialized because conditional
// replacement has to compare and replace one local directory entry.
type Store struct {
	mu                sync.RWMutex
	root              *os.Root
	createDirectories bool
	closed            bool
}

// New opens the configured filesystem root.
func New(configuration providerconfig.Backend) (*Store, error) {
	if configuration.Type != "filesystem" {
		return nil, fmt.Errorf("filesystem backend received type %q", configuration.Type)
	}
	if configuration.Root == "" {
		return nil, errors.New("filesystem backend root is required")
	}
	if configuration.CreateDirectories {
		if err := os.MkdirAll(configuration.Root, 0o750); err != nil {
			return nil, fmt.Errorf("create filesystem backend root: %w", err)
		}
	}
	root, err := os.OpenRoot(configuration.Root)
	if err != nil {
		return nil, fmt.Errorf("open filesystem backend root: %w", err)
	}
	return &Store{
		root:              root,
		createDirectories: configuration.CreateDirectories,
	}, nil
}

// Probe verifies that one configured bucket root is a directory. It creates
// that directory only when createDirectories is explicitly enabled.
func (s *Store) Probe(ctx context.Context, bucket backend.Bucket) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.ensureOpenLocked(); err != nil {
		return err
	}
	directory, err := bucketDirectory(bucket)
	if err != nil {
		return err
	}
	if s.createDirectories {
		if err := s.root.MkdirAll(directory, 0o750); err != nil {
			return fmt.Errorf("create filesystem bucket directory: %w", err)
		}
	}
	info, err := s.root.Stat(directory)
	if err != nil {
		return fmt.Errorf("stat filesystem bucket directory: %w", err)
	}
	if !info.IsDir() {
		return errors.New("filesystem bucket root is not a directory")
	}
	return nil
}

// List returns one lexically ordered page of regular files.
func (s *Store) List(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.ListRequest,
) (backend.ListPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.ensureOpenLocked(); err != nil {
		return backend.ListPage{}, err
	}
	if request.Limit < 0 {
		return backend.ListPage{}, errors.New("filesystem list limit must not be negative")
	}
	if err := validateListPrefix(request.Prefix); err != nil {
		return backend.ListPage{}, err
	}
	if request.StartAfter != "" {
		if err := validateFilesystemKey(request.StartAfter); err != nil {
			return backend.ListPage{}, err
		}
	}
	if request.ContinuationToken != "" {
		if err := validateFilesystemKey(request.ContinuationToken); err != nil {
			return backend.ListPage{}, errors.New("invalid filesystem continuation token")
		}
	}
	base, err := s.requireBucketDirectoryLocked(bucket)
	if err != nil {
		return backend.ListPage{}, err
	}

	limit := request.Limit
	if limit == 0 {
		limit = defaultListLimit
	}
	anchor := request.StartAfter
	if request.ContinuationToken != "" {
		anchor = request.ContinuationToken
	}
	objects := make([]backend.Object, 0, int(limit)+1)
	walkErr := fs.WalkDir(s.root.FS(), base, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || strings.HasPrefix(entry.Name(), temporaryPrefix) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		physical, found := strings.CutPrefix(name, bucket.Name+"/")
		if !found {
			return nil
		}
		logical, found := backend.LogicalKey(bucket, physical)
		if !found || !strings.HasPrefix(logical, request.Prefix) || logical <= anchor {
			return nil
		}
		objects = append(objects, s.objectFromInfoLocked(name, logical, info))
		if len(objects) > int(limit) {
			return fs.SkipAll
		}
		return nil
	})
	if walkErr != nil {
		return backend.ListPage{}, fmt.Errorf("list filesystem objects: %w", walkErr)
	}

	page := backend.ListPage{
		Objects: objects,
		Fields: backend.ObjectFieldKey |
			backend.ObjectFieldContentType |
			backend.ObjectFieldContentEncoding |
			backend.ObjectFieldContentDisposition |
			backend.ObjectFieldCacheControl |
			backend.ObjectFieldContentLength |
			backend.ObjectFieldLastModified |
			backend.ObjectFieldVersionID |
			backend.ObjectFieldMetadata,
	}
	if len(objects) > int(limit) {
		page.Objects = objects[:limit]
		page.ContinuationToken = page.Objects[len(page.Objects)-1].Key
	}
	return page, nil
}

// Close releases the root directory handle. It is idempotent.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.root.Close()
}

func (s *Store) requireBucketDirectoryLocked(bucket backend.Bucket) (string, error) {
	directory, err := bucketDirectory(bucket)
	if err != nil {
		return "", err
	}
	info, err := s.root.Stat(directory)
	if err != nil {
		return "", fmt.Errorf("stat filesystem bucket directory: %w", err)
	}
	if !info.IsDir() {
		return "", errors.New("filesystem bucket root is not a directory")
	}
	return directory, nil
}

func (s *Store) ensureBucketDirectoryLocked(bucket backend.Bucket) (string, error) {
	directory, err := bucketDirectory(bucket)
	if err != nil {
		return "", err
	}
	if s.createDirectories {
		if err := s.root.MkdirAll(directory, 0o750); err != nil {
			return "", fmt.Errorf("create filesystem bucket directory: %w", err)
		}
	}
	return s.requireBucketDirectoryLocked(bucket)
}

func (s *Store) ensureOpenLocked() error {
	if s.closed {
		return errClosed
	}
	return nil
}

func bucketDirectory(bucket backend.Bucket) (string, error) {
	if err := validateBucket(bucket); err != nil {
		return "", err
	}
	directory := bucket.Name
	if bucket.Prefix != "" {
		directory = path.Join(directory, strings.TrimSuffix(bucket.Prefix, "/"))
	}
	return directory, nil
}

func objectPath(bucket backend.Bucket, key string) (string, error) {
	if err := validateBucket(bucket); err != nil {
		return "", err
	}
	if err := validateFilesystemKey(key); err != nil {
		return "", err
	}
	physical, err := backend.PhysicalKey(bucket, key)
	if err != nil {
		return "", err
	}
	return path.Join(bucket.Name, physical), nil
}

func validateBucket(bucket backend.Bucket) error {
	if bucket.Name == "" || bucket.Name == "." || bucket.Name == ".." ||
		strings.ContainsAny(bucket.Name, "/\\") ||
		strings.HasPrefix(bucket.Name, internalDirectory) {
		return errors.New("invalid filesystem bucket directory")
	}
	if bucket.Prefix == "" {
		return nil
	}
	if !strings.HasSuffix(bucket.Prefix, "/") {
		return errors.New("filesystem bucket prefix must end with a slash")
	}
	return validateFilesystemKey(strings.TrimSuffix(bucket.Prefix, "/"))
}

func validateFilesystemKey(key string) error {
	if err := backend.ValidateKey(key); err != nil {
		return err
	}
	for _, segment := range strings.Split(key, "/") {
		if strings.HasPrefix(segment, temporaryPrefix) {
			return &backend.KeyError{Key: key, Reason: "key uses a reserved filesystem segment"}
		}
	}
	return nil
}

func validateListPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	trimmed := strings.TrimSuffix(prefix, "/")
	if trimmed == "" {
		return &backend.KeyError{Key: prefix, Reason: "list prefix is unsafe"}
	}
	return validateFilesystemKey(trimmed)
}

var _ backend.Store = (*Store)(nil)
