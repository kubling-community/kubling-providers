package fsstore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"os"
	"path"
	"strings"
	"time"

	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
)

const (
	metadataVersion  = 1
	maxMetadataBytes = 1024 * 1024
)

type metadataRecord struct {
	Version            int               `json:"version"`
	Path               string            `json:"path"`
	Size               int64             `json:"size"`
	ModifiedUnixNano   int64             `json:"modifiedUnixNano"`
	ETag               string            `json:"etag"`
	Revision           string            `json:"revision,omitempty"`
	UpdatedUnixNano    int64             `json:"updatedUnixNano,omitempty"`
	Checksum           string            `json:"checksum"`
	ContentType        string            `json:"contentType,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

// Stat reads one object's intrinsic and portable metadata.
func (s *Store) Stat(
	ctx context.Context,
	bucket backend.Bucket,
	key string,
) (backend.Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return backend.Object{}, err
	}
	if err := s.ensureOpenLocked(); err != nil {
		return backend.Object{}, err
	}
	name, err := objectPath(bucket, key)
	if err != nil {
		return backend.Object{}, err
	}
	file, info, err := s.openRegularLocked(name)
	if err != nil {
		return backend.Object{}, err
	}
	defer file.Close()
	return s.objectFromFileLocked(ctx, name, key, file, info)
}

// Open returns one complete object or byte range. The returned descriptor keeps
// reading the same inode if a later provider mutation atomically replaces it.
func (s *Store) Open(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.GetRequest,
) (backend.ObjectReader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return backend.ObjectReader{}, err
	}
	if err := s.ensureOpenLocked(); err != nil {
		return backend.ObjectReader{}, err
	}
	if request.Offset < 0 {
		return backend.ObjectReader{}, errors.New("filesystem object offset must not be negative")
	}
	if request.Length != nil && *request.Length < 0 {
		return backend.ObjectReader{}, errors.New("filesystem object length must not be negative")
	}
	name, err := objectPath(bucket, request.Key)
	if err != nil {
		return backend.ObjectReader{}, err
	}
	file, info, err := s.openRegularLocked(name)
	if err != nil {
		return backend.ObjectReader{}, err
	}
	object, err := s.objectFromFileLocked(ctx, name, request.Key, file, info)
	if err != nil {
		file.Close()
		return backend.ObjectReader{}, err
	}
	if request.VersionID != "" ||
		(request.Revision != "" && strings.Trim(request.Revision, "\"") != object.Revision) {
		file.Close()
		return backend.ObjectReader{}, backend.ErrConflict
	}
	if request.Offset > object.Size {
		file.Close()
		return backend.ObjectReader{}, errors.New("filesystem object offset exceeds its size")
	}
	if request.Length != nil && *request.Length == 0 {
		file.Close()
		return backend.ObjectReader{
			Object: object,
			Body:   io.NopCloser(strings.NewReader("")),
		}, nil
	}
	if _, err := file.Seek(request.Offset, io.SeekStart); err != nil {
		file.Close()
		return backend.ObjectReader{}, fmt.Errorf("seek filesystem object: %w", err)
	}
	body := io.Reader(file)
	if request.Length != nil {
		body = io.LimitReader(file, *request.Length)
	}
	return backend.ObjectReader{
		Object: object,
		Body:   &readCloser{Reader: body, Closer: file},
	}, nil
}

// Put writes a complete replacement to a temporary file, synchronizes it, and
// atomically publishes it inside the same directory.
func (s *Store) Put(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.PutRequest,
) (backend.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return backend.Object{}, err
	}
	if err := s.ensureOpenLocked(); err != nil {
		return backend.Object{}, err
	}
	if request.Body == nil {
		return backend.Object{}, errors.New("filesystem object body is required")
	}
	if request.Size != nil && *request.Size < 0 {
		return backend.Object{}, errors.New("filesystem object size must not be negative")
	}
	if request.IfNoneMatch && request.IfRevisionMatch != "" {
		return backend.Object{}, errors.New("filesystem put cannot combine revision and nonexistence conditions")
	}
	if _, err := s.ensureBucketDirectoryLocked(bucket); err != nil {
		return backend.Object{}, err
	}
	name, err := objectPath(bucket, request.Key)
	if err != nil {
		return backend.Object{}, err
	}

	current, currentInfo, exists, err := s.currentObjectLocked(ctx, name, request.Key)
	if err != nil {
		return backend.Object{}, err
	}
	if request.IfNoneMatch && exists {
		return backend.Object{}, backend.ErrConflict
	}
	if request.IfRevisionMatch != "" &&
		(!exists || strings.Trim(request.IfRevisionMatch, "\"") != current.Revision) {
		return backend.Object{}, backend.ErrConflict
	}

	parent := path.Dir(name)
	if err := s.root.MkdirAll(parent, 0o750); err != nil {
		return backend.Object{}, fmt.Errorf("create filesystem object parent: %w", err)
	}
	temporary, err := s.temporaryName(parent)
	if err != nil {
		return backend.Object{}, err
	}
	file, err := s.root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return backend.Object{}, fmt.Errorf("create filesystem object temporary file: %w", err)
	}
	temporaryExists := true
	defer func() {
		if temporaryExists {
			_ = s.root.Remove(temporary)
		}
	}()

	hasher := sha256.New()
	written, copyErr := io.CopyBuffer(
		io.MultiWriter(file, hasher),
		&contextReader{ctx: ctx, reader: request.Body},
		make([]byte, 256*1024),
	)
	if copyErr == nil && request.Size != nil && written != *request.Size {
		copyErr = fmt.Errorf("filesystem object size mismatch: expected %d bytes, received %d", *request.Size, written)
	}
	if copyErr == nil {
		copyErr = file.Sync()
	}
	info, statErr := file.Stat()
	closeErr := file.Close()
	if copyErr != nil {
		return backend.Object{}, fmt.Errorf("write filesystem object: %w", copyErr)
	}
	if statErr != nil {
		return backend.Object{}, fmt.Errorf("stat filesystem object temporary file: %w", statErr)
	}
	if closeErr != nil {
		return backend.Object{}, fmt.Errorf("close filesystem object temporary file: %w", closeErr)
	}

	digest := hasher.Sum(nil)
	revision, err := newRevision()
	if err != nil {
		return backend.Object{}, err
	}
	record := metadataRecord{
		Version:            metadataVersion,
		Path:               name,
		Size:               info.Size(),
		ModifiedUnixNano:   info.ModTime().UnixNano(),
		ETag:               hex.EncodeToString(digest),
		Revision:           revision,
		UpdatedUnixNano:    info.ModTime().UnixNano(),
		Checksum:           "SHA256:" + base64.StdEncoding.EncodeToString(digest),
		ContentType:        request.ContentType,
		ContentEncoding:    request.ContentEncoding,
		ContentDisposition: request.ContentDisposition,
		CacheControl:       request.CacheControl,
		Metadata:           cloneMetadata(request.Metadata),
	}
	if record.ContentType == "" {
		record.ContentType = mime.TypeByExtension(path.Ext(request.Key))
	}
	metadataTemporary, metadataTarget, err := s.writeMetadataTemporaryLocked(record)
	if err != nil {
		return backend.Object{}, err
	}
	metadataTemporaryExists := true
	defer func() {
		if metadataTemporaryExists {
			_ = s.root.Remove(metadataTemporary)
		}
	}()

	if request.IfNoneMatch {
		if err := s.root.Link(temporary, name); err != nil {
			if errors.Is(err, fs.ErrExist) {
				return backend.Object{}, backend.ErrConflict
			}
			return backend.Object{}, fmt.Errorf("publish new filesystem object: %w", err)
		}
		if err := s.root.Remove(temporary); err != nil {
			return backend.Object{}, fmt.Errorf("remove linked filesystem temporary file: %w", err)
		}
		temporaryExists = false
	} else {
		if request.IfRevisionMatch != "" {
			latest, err := s.root.Stat(name)
			if err != nil || !os.SameFile(currentInfo, latest) {
				return backend.Object{}, backend.ErrConflict
			}
		}
		if err := s.root.Rename(temporary, name); err != nil {
			return backend.Object{}, fmt.Errorf("publish filesystem object replacement: %w", err)
		}
		temporaryExists = false
	}
	if err := s.root.Rename(metadataTemporary, metadataTarget); err != nil {
		_ = s.root.Remove(metadataTarget)
		return backend.Object{}, fmt.Errorf("publish filesystem object metadata: %w", err)
	}
	metadataTemporaryExists = false
	if err := s.syncDirectoryLocked(parent); err != nil {
		return backend.Object{}, err
	}
	if err := s.syncDirectoryLocked(path.Dir(metadataTarget)); err != nil {
		return backend.Object{}, err
	}
	return record.object(request.Key, info), nil
}

// UpdateAttributes atomically replaces the provider-owned metadata sidecar
// while preserving the object file and its content identity.
func (s *Store) UpdateAttributes(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.UpdateAttributesRequest,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.ensureOpenLocked(); err != nil {
		return err
	}
	if request.Fields.Empty() {
		return errors.New("filesystem attribute update requires at least one field")
	}
	if request.IfRevisionMatch == "" {
		return errors.New("filesystem attribute update requires a revision")
	}
	name, err := objectPath(bucket, request.Key)
	if err != nil {
		return err
	}
	current, info, exists, err := s.currentObjectLocked(ctx, name, request.Key)
	if err != nil {
		return err
	}
	if !exists {
		return backend.ErrNotFound
	}
	if strings.Trim(request.IfRevisionMatch, "\"") != current.Revision {
		return backend.ErrConflict
	}
	revision, err := newRevision()
	if err != nil {
		return err
	}
	record := metadataRecord{
		Version:            metadataVersion,
		Path:               name,
		Size:               info.Size(),
		ModifiedUnixNano:   info.ModTime().UnixNano(),
		ETag:               current.ETag,
		Revision:           revision,
		UpdatedUnixNano:    time.Now().UTC().UnixNano(),
		Checksum:           current.Checksum,
		ContentType:        request.ContentType,
		ContentEncoding:    request.ContentEncoding,
		ContentDisposition: request.ContentDisposition,
		CacheControl:       request.CacheControl,
		Metadata:           cloneMetadata(request.Metadata),
	}
	metadataTemporary, metadataTarget, err := s.writeMetadataTemporaryLocked(record)
	if err != nil {
		return err
	}
	metadataTemporaryExists := true
	defer func() {
		if metadataTemporaryExists {
			_ = s.root.Remove(metadataTemporary)
		}
	}()
	latest, latestInfo, latestExists, err := s.currentObjectLocked(ctx, name, request.Key)
	if err != nil {
		return err
	}
	if !latestExists || !os.SameFile(info, latestInfo) || latest.Revision != current.Revision {
		return backend.ErrConflict
	}
	if err := s.root.Rename(metadataTemporary, metadataTarget); err != nil {
		return fmt.Errorf("publish filesystem object attributes: %w", err)
	}
	metadataTemporaryExists = false
	return s.syncDirectoryLocked(path.Dir(metadataTarget))
}

// Delete removes one exact object and its provider-owned metadata sidecar.
func (s *Store) Delete(
	ctx context.Context,
	bucket backend.Bucket,
	request backend.DeleteRequest,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.ensureOpenLocked(); err != nil {
		return err
	}
	if request.VersionID != "" {
		return backend.ErrConflict
	}
	name, err := objectPath(bucket, request.Key)
	if err != nil {
		return err
	}
	info, err := s.root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return backend.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("stat filesystem object for delete: %w", err)
	}
	if !info.Mode().IsRegular() {
		return backend.ErrNotFound
	}
	if request.IfRevisionMatch != "" {
		current, _, exists, err := s.currentObjectLocked(ctx, name, request.Key)
		if err != nil {
			return err
		}
		if !exists || strings.Trim(request.IfRevisionMatch, "\"") != current.Revision {
			return backend.ErrConflict
		}
	}
	if err := s.root.Remove(name); err != nil {
		return fmt.Errorf("delete filesystem object: %w", err)
	}
	metadataName := metadataPath(name)
	if err := s.root.Remove(metadataName); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("delete filesystem object metadata: %w", err)
	}
	return s.syncDirectoryLocked(path.Dir(name))
}

func (s *Store) currentObjectLocked(
	ctx context.Context,
	name string,
	key string,
) (backend.Object, fs.FileInfo, bool, error) {
	file, info, err := s.openRegularLocked(name)
	if errors.Is(err, backend.ErrNotFound) {
		return backend.Object{}, nil, false, nil
	}
	if err != nil {
		return backend.Object{}, nil, false, err
	}
	defer file.Close()
	object, err := s.objectFromFileLocked(ctx, name, key, file, info)
	return object, info, true, err
}

func (s *Store) openRegularLocked(name string) (*os.File, fs.FileInfo, error) {
	linkInfo, err := s.root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, backend.ErrNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("stat filesystem object: %w", err)
	}
	if linkInfo.Mode()&os.ModeSymlink != 0 || !linkInfo.Mode().IsRegular() {
		return nil, nil, backend.ErrNotFound
	}
	file, err := s.root.Open(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, backend.ErrNotFound
	}
	if err != nil {
		return nil, nil, fmt.Errorf("open filesystem object: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("stat open filesystem object: %w", err)
	}
	if !info.Mode().IsRegular() {
		file.Close()
		return nil, nil, backend.ErrNotFound
	}
	return file, info, nil
}

func (s *Store) objectFromFileLocked(
	ctx context.Context,
	name string,
	key string,
	file *os.File,
	info fs.FileInfo,
) (backend.Object, error) {
	if object, found := s.objectFromMetadataLocked(name, key, info); found {
		return object, nil
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return backend.Object{}, fmt.Errorf("seek filesystem object for checksum: %w", err)
	}
	hasher := sha256.New()
	if _, err := io.CopyBuffer(
		hasher,
		&contextReader{ctx: ctx, reader: file},
		make([]byte, 256*1024),
	); err != nil {
		return backend.Object{}, fmt.Errorf("checksum filesystem object: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return backend.Object{}, fmt.Errorf("rewind filesystem object: %w", err)
	}
	digest := hasher.Sum(nil)
	etag := hex.EncodeToString(digest)
	return backend.Object{
		Key:          key,
		Size:         info.Size(),
		ETag:         etag,
		Revision:     etag,
		LastModified: info.ModTime(),
		ContentType:  mime.TypeByExtension(path.Ext(key)),
		Checksum:     "SHA256:" + base64.StdEncoding.EncodeToString(digest),
	}, nil
}

func (s *Store) objectFromInfoLocked(name string, key string, info fs.FileInfo) backend.Object {
	if object, found := s.objectFromMetadataLocked(name, key, info); found {
		return object
	}
	return backend.Object{
		Key:          key,
		Size:         info.Size(),
		LastModified: info.ModTime(),
		ContentType:  mime.TypeByExtension(path.Ext(key)),
	}
}

func (s *Store) objectFromMetadataLocked(
	name string,
	key string,
	info fs.FileInfo,
) (backend.Object, bool) {
	serialized, err := s.root.ReadFile(metadataPath(name))
	if err != nil || len(serialized) > maxMetadataBytes {
		return backend.Object{}, false
	}
	var record metadataRecord
	if json.Unmarshal(serialized, &record) != nil ||
		record.Version != metadataVersion ||
		record.Path != name ||
		record.Size != info.Size() ||
		record.ModifiedUnixNano != info.ModTime().UnixNano() ||
		record.ETag == "" {
		return backend.Object{}, false
	}
	return record.object(key, info), true
}

func (s *Store) writeMetadataTemporaryLocked(record metadataRecord) (string, string, error) {
	target := metadataPath(record.Path)
	parent := path.Dir(target)
	if err := s.root.MkdirAll(parent, 0o750); err != nil {
		return "", "", fmt.Errorf("create filesystem metadata directory: %w", err)
	}
	temporary, err := s.temporaryName(parent)
	if err != nil {
		return "", "", err
	}
	serialized, err := json.Marshal(record)
	if err != nil {
		return "", "", fmt.Errorf("marshal filesystem object metadata: %w", err)
	}
	serialized = append(serialized, '\n')
	if len(serialized) > maxMetadataBytes {
		return "", "", errors.New("filesystem object metadata exceeds 1 MiB")
	}
	file, err := s.root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return "", "", fmt.Errorf("create filesystem metadata temporary file: %w", err)
	}
	_, writeErr := file.Write(serialized)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil {
		_ = s.root.Remove(temporary)
		return "", "", fmt.Errorf("write filesystem object metadata: %w", writeErr)
	}
	if closeErr != nil {
		_ = s.root.Remove(temporary)
		return "", "", fmt.Errorf("close filesystem object metadata: %w", closeErr)
	}
	return temporary, target, nil
}

func (s *Store) temporaryName(parent string) (string, error) {
	for range 10 {
		value := make([]byte, 12)
		if _, err := rand.Read(value); err != nil {
			return "", fmt.Errorf("create filesystem temporary name: %w", err)
		}
		name := path.Join(parent, temporaryPrefix+hex.EncodeToString(value))
		if _, err := s.root.Lstat(name); errors.Is(err, fs.ErrNotExist) {
			return name, nil
		} else if err != nil {
			return "", fmt.Errorf("check filesystem temporary name: %w", err)
		}
	}
	return "", errors.New("allocate unique filesystem temporary name")
}

func (s *Store) syncDirectoryLocked(name string) error {
	directory, err := s.root.Open(name)
	if err != nil {
		return fmt.Errorf("open filesystem directory for sync: %w", err)
	}
	err = directory.Sync()
	closeErr := directory.Close()
	if err != nil {
		return fmt.Errorf("sync filesystem directory: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close filesystem directory after sync: %w", closeErr)
	}
	return nil
}

func metadataPath(objectName string) string {
	digest := sha256.Sum256([]byte(objectName))
	encoded := hex.EncodeToString(digest[:])
	return path.Join(internalDirectory, "metadata", encoded[:2], encoded[2:]+".json")
}

func (record metadataRecord) object(key string, info fs.FileInfo) backend.Object {
	revision := record.Revision
	if revision == "" {
		revision = record.ETag
	}
	lastModified := info.ModTime()
	if record.UpdatedUnixNano != 0 {
		lastModified = time.Unix(0, record.UpdatedUnixNano).UTC()
	}
	return backend.Object{
		Key:                key,
		Size:               info.Size(),
		ETag:               record.ETag,
		Revision:           revision,
		LastModified:       lastModified,
		ContentType:        record.ContentType,
		ContentEncoding:    record.ContentEncoding,
		ContentDisposition: record.ContentDisposition,
		CacheControl:       record.CacheControl,
		Checksum:           record.Checksum,
		Metadata:           cloneMetadata(record.Metadata),
	}
}

func newRevision() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("create filesystem object revision: %w", err)
	}
	return hex.EncodeToString(value), nil
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

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(value []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(value)
}

type readCloser struct {
	io.Reader
	io.Closer
}

var _ io.ReadCloser = (*readCloser)(nil)
