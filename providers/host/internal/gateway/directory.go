package gateway

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	gatewaypb "github.com/kubling-community/kubling-providers/providers/host/internal/gatewaypb"
	"github.com/kubling-community/kubling-providers/providers/host/internal/model"
	"google.golang.org/protobuf/proto"
)

var (
	ErrSessionClosed = errors.New("agent gateway session is closed")
	ErrSessionBusy   = errors.New("agent gateway session reached its scan limit")
	ErrScanNotActive = errors.New("agent scan is not active")
	ErrScanRetired   = errors.New("agent scan is already retired")
)

const (
	scanEventBufferLimit = 2
	retiredScanLimit     = 4096
	maxScanBatchRows     = 1024
	maxScanBatchBytes    = 3 << 20
)

type responseSender interface {
	Send(*gatewaypb.ConnectResponse) error
}

// Session serializes provider messages to one authenticated agent stream.
type Session struct {
	stateMu  sync.RWMutex
	snapshot model.HostSnapshot
	sender   responseSender

	sendMu    sync.Mutex
	done      chan struct{}
	closeOnce sync.Once

	scanMu       sync.Mutex
	scans        map[string]*Scan
	retiredScans map[string]struct{}
	retiredOrder []string
}

// ScanEvent is one ordered message received for an active scan.
type ScanEvent struct {
	Batch    *gatewaypb.ScanBatch
	Finished *gatewaypb.ScanFinished
}

// Scan represents one provider-initiated operation on an agent session.
type Scan struct {
	session *Session
	id      string

	mu             sync.Mutex
	nextBatchIndex uint64
	terminal       bool
	events         chan ScanEvent
	abandoned      chan struct{}
	abandonOnce    sync.Once
}

// Snapshot returns detached identity and session state.
func (s *Session) Snapshot() model.HostSnapshot {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()
	return s.snapshot.Clone()
}

// Send writes one provider message without concurrent writes to the gRPC
// stream.
func (s *Session) Send(response *gatewaypb.ConnectResponse) error {
	if response == nil {
		return errors.New("agent gateway response is required")
	}
	if sessionClosed(s.done) {
		return ErrSessionClosed
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	if sessionClosed(s.done) {
		return ErrSessionClosed
	}
	return s.sender.Send(response)
}

// Done closes when the session is replaced or unbound.
func (s *Session) Done() <-chan struct{} {
	return s.done
}

// StartScan registers and dispatches one scan to this exact session.
func (s *Session) StartScan(request *gatewaypb.ScanRequest) (*Scan, error) {
	if request == nil {
		return nil, errors.New("agent scan request is required")
	}
	if strings.TrimSpace(request.GetScanId()) == "" {
		return nil, errors.New("agent scan ID is required")
	}
	if request.GetTable() == gatewaypb.HostTable_HOST_TABLE_UNSPECIFIED {
		return nil, errors.New("agent scan table is required")
	}
	if sessionClosed(s.done) {
		return nil, ErrSessionClosed
	}

	snapshot := s.Snapshot()
	if snapshot.Session == nil {
		return nil, ErrSessionClosed
	}
	s.scanMu.Lock()
	if sessionClosed(s.done) {
		s.scanMu.Unlock()
		return nil, ErrSessionClosed
	}
	if _, exists := s.scans[request.GetScanId()]; exists {
		s.scanMu.Unlock()
		return nil, fmt.Errorf("agent scan %q is already active", request.GetScanId())
	}
	if _, retired := s.retiredScans[request.GetScanId()]; retired {
		s.scanMu.Unlock()
		return nil, fmt.Errorf("agent scan %q was already used", request.GetScanId())
	}
	if uint32(len(s.scans)) >= snapshot.Session.MaxConcurrentScans {
		s.scanMu.Unlock()
		return nil, ErrSessionBusy
	}
	scan := &Scan{
		session:   s,
		id:        request.GetScanId(),
		events:    make(chan ScanEvent, scanEventBufferLimit),
		abandoned: make(chan struct{}),
	}
	s.scans[scan.id] = scan
	s.scanMu.Unlock()

	response := &gatewaypb.ConnectResponse{
		Payload: &gatewaypb.ConnectResponse_ScanRequest{
			ScanRequest: proto.Clone(request).(*gatewaypb.ScanRequest),
		},
	}
	if err := s.Send(response); err != nil {
		s.retireScan(scan)
		scan.abandon()
		return nil, err
	}
	return scan, nil
}

// Next waits for the next ordered batch or terminal message.
func (s *Scan) Next(ctx context.Context) (ScanEvent, error) {
	select {
	case event := <-s.events:
		return event, nil
	case <-s.session.Done():
		return ScanEvent{}, ErrSessionClosed
	case <-s.abandoned:
		return ScanEvent{}, ErrScanRetired
	case <-ctx.Done():
		return ScanEvent{}, ctx.Err()
	}
}

// Cancel retires the local correlation before requesting cooperative agent
// cancellation. Later batches for the scan are discarded without invalidating
// the otherwise healthy agent session.
func (s *Scan) Cancel(reason gatewaypb.CancelReason) error {
	if reason == gatewaypb.CancelReason_CANCEL_REASON_UNSPECIFIED {
		return errors.New("agent scan cancellation reason is required")
	}
	s.mu.Lock()
	alreadyTerminal := s.terminal
	s.terminal = true
	s.mu.Unlock()
	s.session.retireScan(s)
	s.abandon()
	if alreadyTerminal {
		return nil
	}
	return s.session.Send(&gatewaypb.ConnectResponse{
		Payload: &gatewaypb.ConnectResponse_CancelScan{
			CancelScan: &gatewaypb.CancelScan{ScanId: s.id, Reason: reason},
		},
	})
}

func (s *Session) deliverBatch(
	ctx context.Context,
	batch *gatewaypb.ScanBatch,
) error {
	if batch == nil || strings.TrimSpace(batch.GetScanId()) == "" {
		return errors.New("agent scan batch and scan ID are required")
	}
	if len(batch.GetRows()) > maxScanBatchRows {
		return fmt.Errorf(
			"agent scan batch has %d rows, maximum is %d",
			len(batch.GetRows()),
			maxScanBatchRows,
		)
	}
	if size := proto.Size(batch); size > maxScanBatchBytes {
		return fmt.Errorf(
			"agent scan batch is %d bytes, maximum is %d",
			size,
			maxScanBatchBytes,
		)
	}
	scan, err := s.activeScan(batch.GetScanId())
	if err != nil {
		return err
	}

	scan.mu.Lock()
	if scan.terminal {
		scan.mu.Unlock()
		return ErrScanRetired
	}
	if batch.GetBatchIndex() != scan.nextBatchIndex {
		want := scan.nextBatchIndex
		scan.mu.Unlock()
		return fmt.Errorf(
			"agent scan %q batch index is %d, want %d",
			batch.GetScanId(),
			batch.GetBatchIndex(),
			want,
		)
	}
	scan.nextBatchIndex++
	scan.mu.Unlock()

	select {
	case scan.events <- ScanEvent{Batch: proto.Clone(batch).(*gatewaypb.ScanBatch)}:
		return nil
	case <-s.done:
		return ErrSessionClosed
	case <-scan.abandoned:
		return ErrScanRetired
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Session) finishScan(
	ctx context.Context,
	finished *gatewaypb.ScanFinished,
) error {
	if finished == nil || strings.TrimSpace(finished.GetScanId()) == "" {
		return errors.New("agent scan completion and scan ID are required")
	}
	scan, err := s.activeScan(finished.GetScanId())
	if err != nil {
		return err
	}

	scan.mu.Lock()
	if scan.terminal {
		scan.mu.Unlock()
		return ErrScanRetired
	}
	scan.terminal = true
	scan.mu.Unlock()
	s.retireScan(scan)

	select {
	case scan.events <- ScanEvent{Finished: proto.Clone(finished).(*gatewaypb.ScanFinished)}:
		return nil
	case <-s.done:
		return ErrSessionClosed
	case <-scan.abandoned:
		return ErrScanRetired
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Session) activeScan(scanID string) (*Scan, error) {
	s.scanMu.Lock()
	defer s.scanMu.Unlock()
	if scan := s.scans[scanID]; scan != nil {
		return scan, nil
	}
	if _, retired := s.retiredScans[scanID]; retired {
		return nil, ErrScanRetired
	}
	return nil, ErrScanNotActive
}

func (s *Session) retireScan(scan *Scan) {
	s.scanMu.Lock()
	if s.scans[scan.id] == scan {
		delete(s.scans, scan.id)
		if _, exists := s.retiredScans[scan.id]; !exists {
			s.retiredScans[scan.id] = struct{}{}
			s.retiredOrder = append(s.retiredOrder, scan.id)
			if len(s.retiredOrder) > retiredScanLimit {
				oldest := s.retiredOrder[0]
				s.retiredOrder = s.retiredOrder[1:]
				delete(s.retiredScans, oldest)
			}
		}
	}
	s.scanMu.Unlock()
}

func (s *Scan) abandon() {
	s.abandonOnce.Do(func() { close(s.abandoned) })
}

func (s *Session) update(snapshot model.HostSnapshot) error {
	current := s.Snapshot()
	if current.Session == nil || snapshot.Session == nil ||
		current.Session.ID != snapshot.Session.ID ||
		current.Session.Generation != snapshot.Session.Generation ||
		current.Identity.Key != snapshot.Identity.Key {
		return errors.New("agent gateway session update does not match binding")
	}
	s.stateMu.Lock()
	s.snapshot = snapshot.Clone()
	s.stateMu.Unlock()
	return nil
}

func (s *Session) close() {
	s.closeOnce.Do(func() { close(s.done) })
}

// Directory tracks the single live transport session for each host.
type Directory struct {
	mu      sync.RWMutex
	current map[model.HostKey]*Session
	byID    map[string]*Session
}

// NewDirectory creates an empty live-session directory.
func NewDirectory() *Directory {
	return &Directory{
		current: make(map[model.HostKey]*Session),
		byID:    make(map[string]*Session),
	}
}

// Bind atomically installs a stream and closes the previous binding for the
// same host, if one exists.
func (d *Directory) Bind(
	snapshot model.HostSnapshot,
	sender responseSender,
) (*Session, error) {
	if sender == nil {
		return nil, errors.New("agent gateway sender is required")
	}
	if snapshot.Session == nil {
		return nil, errors.New("agent gateway snapshot session is required")
	}
	if strings.TrimSpace(snapshot.Session.ID) == "" {
		return nil, errors.New("agent gateway session ID is required")
	}
	if snapshot.Session.Generation == 0 {
		return nil, errors.New("agent gateway session generation is required")
	}
	if snapshot.Identity.Key != snapshot.Session.Host {
		return nil, errors.New("agent gateway identity and session host differ")
	}

	session := &Session{
		snapshot:     snapshot.Clone(),
		sender:       sender,
		done:         make(chan struct{}),
		scans:        make(map[string]*Scan),
		retiredScans: make(map[string]struct{}),
		retiredOrder: make([]string, 0),
	}
	d.mu.Lock()
	if _, exists := d.byID[snapshot.Session.ID]; exists {
		d.mu.Unlock()
		return nil, errors.New("agent gateway session ID is already bound")
	}
	previous := d.current[snapshot.Identity.Key]
	if previous != nil {
		delete(d.byID, previous.Snapshot().Session.ID)
	}
	d.current[snapshot.Identity.Key] = session
	d.byID[snapshot.Session.ID] = session
	d.mu.Unlock()

	if previous != nil {
		previous.close()
	}
	return session, nil
}

// Unbind removes only the exact binding supplied. A stale stream cannot remove
// its replacement.
func (d *Directory) Unbind(session *Session) {
	if session == nil {
		return
	}
	snapshot := session.Snapshot()
	d.mu.Lock()
	if d.byID[snapshot.Session.ID] == session {
		delete(d.byID, snapshot.Session.ID)
	}
	if d.current[snapshot.Identity.Key] == session {
		delete(d.current, snapshot.Identity.Key)
	}
	d.mu.Unlock()
	session.close()
}

// Current resolves the current live binding for a host.
func (d *Directory) Current(key model.HostKey) (*Session, bool) {
	d.mu.RLock()
	session, exists := d.current[key]
	d.mu.RUnlock()
	return session, exists
}

// Resolve resolves a live binding by opaque session identifier.
func (d *Directory) Resolve(sessionID string) (*Session, bool) {
	d.mu.RLock()
	session, exists := d.byID[sessionID]
	d.mu.RUnlock()
	return session, exists
}

// IsCurrent reports whether session is still the live binding for its host.
func (d *Directory) IsCurrent(session *Session) bool {
	if session == nil {
		return false
	}
	snapshot := session.Snapshot()
	d.mu.RLock()
	current := d.current[snapshot.Identity.Key]
	d.mu.RUnlock()
	return current == session
}

func sessionClosed(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}
