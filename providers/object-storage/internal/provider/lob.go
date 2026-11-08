package provider

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/backend"
	"github.com/kubling-community/kubling-providers/providers/object-storage/internal/catalog"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const lobIDBytes = 16

type lobReference struct {
	route     catalog.Bucket
	key       string
	size      uint64
	versionID string
	revision  string
	expiresAt time.Time
}

func (c *Connection) newLobValue(
	route catalog.Bucket,
	object backend.Object,
) (*kublingv1.Value, error) {
	if object.Size < 0 {
		return nil, status.Error(codes.DataLoss, "object backend returned a negative size")
	}
	size := uint64(object.Size)
	if size > c.lobPolicy.MaxBytes {
		return nil, status.Error(codes.ResourceExhausted, "object exceeds the advertised LOB size limit")
	}
	if object.VersionID == "" && object.Revision == "" {
		return nil, status.Error(
			codes.FailedPrecondition,
			"object backend did not provide an immutable version or entity tag",
		)
	}

	expiresAt := time.Now().Add(c.lobPolicy.Retention)
	var id string
	for {
		candidate, err := newLobID()
		if err != nil {
			return nil, status.Error(codes.Internal, "create object LOB identifier")
		}
		c.lobMu.Lock()
		c.purgeExpiredLobsLocked(time.Now())
		_, exists := c.lobs[candidate]
		if !exists {
			c.lobs[candidate] = lobReference{
				route:     route,
				key:       object.Key,
				size:      size,
				versionID: object.VersionID,
				revision:  object.Revision,
				expiresAt: expiresAt,
			}
			id = candidate
		}
		c.lobMu.Unlock()
		if !exists {
			break
		}
	}

	expiresAtUnixMs := expiresAt.UnixMilli()
	return &kublingv1.Value{Kind: &kublingv1.Value_LobReference{
		LobReference: &kublingv1.LobReference{
			LobId:           id,
			Type:            kublingv1.ValueType_VALUE_TYPE_BLOB,
			SizeBytes:       &size,
			ExpiresAtUnixMs: &expiresAtUnixMs,
		},
	}}, nil
}

// ReadLob opens one immutable object range through its connection-scoped reference.
func (c *Connection) ReadLob(
	ctx context.Context,
	request *providerv1.ReadLobRequest,
) (providersdk.LobStream, error) {
	if err := c.lockOpen(ctx); err != nil {
		return nil, err
	}
	defer c.unlockOpen()
	if request == nil || request.GetLobId() == "" {
		return nil, status.Error(codes.InvalidArgument, "LOB identifier is required")
	}

	now := time.Now()
	c.lobMu.Lock()
	c.purgeExpiredLobsLocked(now)
	reference, found := c.lobs[request.GetLobId()]
	c.lobMu.Unlock()
	if !found {
		return nil, status.Error(codes.NotFound, "provider LOB was not found")
	}
	if request.GetOffset() > reference.size {
		return nil, status.Error(codes.InvalidArgument, "LOB offset exceeds its size")
	}

	length := reference.size - request.GetOffset()
	if request.Length != nil && request.GetLength() < length {
		length = request.GetLength()
	}
	chunkSize := c.lobPolicy.MaxChunkBytes
	if requested := request.GetMaxChunkBytes(); requested > 0 && requested < chunkSize {
		chunkSize = requested
	}
	stream := &objectLobStream{
		offset:    request.GetOffset(),
		remaining: length,
		chunkSize: int(chunkSize),
	}
	if length == 0 {
		return stream, nil
	}

	backendLength := int64(length)
	reader, err := c.stores[reference.route.BackendName].Open(
		ctx,
		reference.route.Target,
		backend.GetRequest{
			Key:       reference.key,
			Offset:    int64(request.GetOffset()),
			Length:    &backendLength,
			VersionID: reference.versionID,
			Revision:  reference.revision,
		},
	)
	if err != nil {
		return nil, backendStatus("open object LOB", err)
	}
	stream.body = reader.Body
	return stream, nil
}

// ReleaseLob idempotently releases one connection-scoped object reference.
func (c *Connection) ReleaseLob(
	ctx context.Context,
	request *providerv1.ReleaseLobRequest,
) error {
	if err := c.lockOpen(ctx); err != nil {
		return err
	}
	defer c.unlockOpen()
	if request == nil || request.GetLobId() == "" {
		return status.Error(codes.InvalidArgument, "LOB identifier is required")
	}
	c.lobMu.Lock()
	delete(c.lobs, request.GetLobId())
	c.lobMu.Unlock()
	return nil
}

func (c *Connection) purgeExpiredLobsLocked(now time.Time) {
	for id, reference := range c.lobs {
		if !now.Before(reference.expiresAt) {
			delete(c.lobs, id)
		}
	}
}

type objectLobStream struct {
	mu sync.Mutex

	body      io.ReadCloser
	offset    uint64
	remaining uint64
	chunkSize int
	done      bool
	closed    bool
}

func (s *objectLobStream) Next(ctx context.Context) (*providerv1.ReadLobResponse, error) {
	if err := contextStatus(ctx); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.done {
		return nil, io.EOF
	}
	if s.remaining == 0 {
		s.done = true
		return &providerv1.ReadLobResponse{Offset: s.offset, EndOfRead: true}, nil
	}

	size := uint64(s.chunkSize)
	if size > s.remaining {
		size = s.remaining
	}
	data := make([]byte, int(size))
	read, err := io.ReadFull(s.body, data)
	if err != nil {
		_ = s.closeBody()
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, status.Error(codes.DataLoss, "object ended before the referenced LOB range")
		}
		return nil, status.Error(codes.Unavailable, "read object LOB failed")
	}
	data = data[:read]
	response := &providerv1.ReadLobResponse{
		Offset: s.offset,
		Data:   data,
	}
	s.offset += uint64(read)
	s.remaining -= uint64(read)
	response.EndOfRead = s.remaining == 0
	if response.EndOfRead {
		s.done = true
		if err := s.closeBody(); err != nil {
			return nil, status.Error(codes.Unavailable, "close object LOB failed")
		}
	}
	return response, nil
}

func (s *objectLobStream) Close() error {
	s.mu.Lock()
	s.closed = true
	err := s.closeBody()
	s.mu.Unlock()
	return err
}

func (s *objectLobStream) closeBody() error {
	if s.body == nil {
		return nil
	}
	err := s.body.Close()
	s.body = nil
	return err
}

func newLobID() (string, error) {
	value := make([]byte, lobIDBytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

var _ providersdk.LobStream = (*objectLobStream)(nil)
