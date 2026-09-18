package inmemory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"sync"
	"time"
	"unicode/utf8"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	inMemoryMaxLobChunkBytes = 64 * 1024
	inMemoryMaxLobBytes      = 1024 * 1024
	inMemoryLobRetention     = 5 * time.Minute
	inMemoryLobIDBytes       = 16
)

type inMemoryLob struct {
	data      []byte
	expiresAt time.Time
}

type inMemoryLobStream struct {
	mu        sync.Mutex
	data      []byte
	offset    uint64
	chunkSize int
	done      bool
	closed    bool
}

func (c *Connection) newLobValue(
	valueType kublingv1.ValueType,
	data []byte,
) (*kublingv1.Value, error) {
	if valueType == kublingv1.ValueType_VALUE_TYPE_CLOB && !utf8.Valid(data) {
		return nil, status.Error(codes.Internal, "in-memory CLOB is not valid UTF-8")
	}
	if uint64(len(data)) > inMemoryMaxLobBytes {
		return nil, status.Error(
			codes.ResourceExhausted,
			"in-memory LOB exceeds the advertised maximum size",
		)
	}

	expiresAt := time.Now().Add(inMemoryLobRetention)
	var lobID string
	for {
		candidate, err := newInMemoryLobID()
		if err != nil {
			return nil, status.Error(codes.Internal, "create in-memory LOB identifier")
		}
		c.lobMu.Lock()
		_, exists := c.lobs[candidate]
		if !exists {
			c.lobs[candidate] = inMemoryLob{
				data:      append([]byte(nil), data...),
				expiresAt: expiresAt,
			}
			lobID = candidate
		}
		c.lobMu.Unlock()
		if !exists {
			break
		}
	}

	sizeBytes := uint64(len(data))
	expiresAtUnixMs := expiresAt.UnixMilli()
	return &kublingv1.Value{Kind: &kublingv1.Value_LobReference{
		LobReference: &kublingv1.LobReference{
			LobId:           lobID,
			Type:            valueType,
			SizeBytes:       &sizeBytes,
			ExpiresAtUnixMs: &expiresAtUnixMs,
		},
	}}, nil
}

func (c *Connection) ReadLob(
	ctx context.Context,
	request *providerv1.ReadLobRequest,
) (providersdk.LobStream, error) {
	if err := c.lockOpen(); err != nil {
		return nil, err
	}
	defer c.unlockOpen()
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.lobMu.Lock()
	lob, found := c.lobs[request.GetLobId()]
	if found && !time.Now().Before(lob.expiresAt) {
		delete(c.lobs, request.GetLobId())
		found = false
	}
	c.lobMu.Unlock()
	if !found {
		return nil, status.Error(codes.NotFound, "provider LOB was not found")
	}

	offset := request.GetOffset()
	size := uint64(len(lob.data))
	if offset > size {
		return nil, status.Error(codes.InvalidArgument, "LOB offset exceeds its size")
	}
	end := size
	if request.Length != nil && request.GetLength() < size-offset {
		end = offset + request.GetLength()
	}
	chunkSize := inMemoryMaxLobChunkBytes
	if requested := request.GetMaxChunkBytes(); requested > 0 && requested < uint32(chunkSize) {
		chunkSize = int(requested)
	}

	return &inMemoryLobStream{
		data:      append([]byte(nil), lob.data[offset:end]...),
		offset:    offset,
		chunkSize: chunkSize,
	}, nil
}

func (c *Connection) ReleaseLob(
	ctx context.Context,
	request *providerv1.ReleaseLobRequest,
) error {
	if err := c.lockOpen(); err != nil {
		return err
	}
	defer c.unlockOpen()
	if err := ctx.Err(); err != nil {
		return err
	}

	c.lobMu.Lock()
	delete(c.lobs, request.GetLobId())
	c.lobMu.Unlock()
	return nil
}

func (s *inMemoryLobStream) Next(
	ctx context.Context,
) (*providerv1.ReadLobResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.done {
		return nil, io.EOF
	}

	chunkLength := len(s.data)
	if chunkLength > s.chunkSize {
		chunkLength = s.chunkSize
	}
	response := &providerv1.ReadLobResponse{
		Offset:    s.offset,
		Data:      append([]byte(nil), s.data[:chunkLength]...),
		EndOfRead: chunkLength == len(s.data),
	}
	s.offset += uint64(chunkLength)
	s.data = s.data[chunkLength:]
	s.done = response.EndOfRead

	return response, nil
}

func (s *inMemoryLobStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.data = nil
	s.mu.Unlock()
	return nil
}

func newInMemoryLobID() (string, error) {
	value := make([]byte, inMemoryLobIDBytes)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}

	return hex.EncodeToString(value), nil
}

var _ providersdk.LobStream = (*inMemoryLobStream)(nil)
