// Package discovery maintains the provider's stable BACnet device registry.
package discovery

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/netip"
	"time"

	"github.com/kubling-community/kubling-providers/providers/bacnet/internal/config"
	bacnet "github.com/otfabric/go-bacnet"
	"github.com/otfabric/go-bacnet/service"
)

const (
	IssueDiscoveryFailed       = "DISCOVERY_FAILED"
	IssueIdentityReadFailed    = "IDENTITY_READ_FAILED"
	IssuePropertyUnavailable   = "PROPERTY_UNAVAILABLE"
	IssueDuplicateInstance     = "DUPLICATE_DEVICE_INSTANCE"
	IssueProfileSelection      = "PROFILE_SELECTION_FAILED"
	IssueAttachmentUnavailable = "ATTACHMENT_UNAVAILABLE"
)

// Issue is one structured, safe diagnostic associated with discovery.
type Issue struct {
	Code       string
	Message    string
	PropertyID uint32
}

// Route is the canonical path used for confirmed BACnet requests.
type Route struct {
	Endpoint      netip.AddrPort
	RemoteNetwork uint16
	MAC           []byte
	MaxAPDU       uint16
}

// Canonical returns a deterministic route identity.
func (r Route) Canonical() string {
	if r.RemoteNetwork == 0 {
		return r.Endpoint.String()
	}
	return fmt.Sprintf("net=%d:%s@%s", r.RemoteNetwork, hex.EncodeToString(r.MAC), r.Endpoint)
}

// Observation is one device sighting or configured static route.
type Observation struct {
	DeviceInstance   uint32
	Route            Route
	LastSeen         time.Time
	VendorIdentifier uint32
	VendorKnown      bool
	Static           bool
}

// Identity contains Device Object properties needed for profile selection and
// operational inventory.
type Identity struct {
	VendorIdentifier           uint32
	VendorName                 string
	ModelName                  string
	FirmwareRevision           string
	ApplicationSoftwareVersion string
	ProtocolVersion            uint32
	ProtocolRevision           uint32
}

// Device is one unambiguous, profile-selected device in a logical scope.
type Device struct {
	Namespace    string
	ScopeID      string
	AttachmentID string
	Instance     uint32
	Route        Route
	Identity     Identity
	ProfileID    string
	LastSeen     time.Time
	Static       bool
	Issues       []Issue
}

// Duplicate records all conflicting routes for one Device Instance.
type Duplicate struct {
	DeviceInstance uint32
	Routes         []Route
}

// ScopeSnapshot is one atomically replaced scope registry generation.
type ScopeSnapshot struct {
	Namespace    string
	ScopeID      string
	AttachmentID string
	LastAttempt  time.Time
	LastSuccess  time.Time
	Devices      []Device
	Duplicates   []Duplicate
	Issues       []Issue
}

// Snapshot is a detached, stable view suitable for one query.
type Snapshot struct {
	Generation  uint64
	CapturedAt  time.Time
	Attachments []AttachmentSnapshot
	Scopes      []ScopeSnapshot
}

// AttachmentSnapshot reports the current physical BACnet/IP endpoint state.
type AttachmentSnapshot struct {
	ID                      string
	LocalEndpoint           netip.AddrPort
	ForeignDevice           bool
	ForeignDeviceRegistered bool
}

// Client is the narrow BACnet transport surface used by the registry and
// single-device query execution.
type Client interface {
	Discover(context.Context, config.Discovery) ([]Observation, error)
	Identify(context.Context, Observation) (Identity, []Issue, error)
	ReadObjectList(context.Context, Route, uint32, int) ([]bacnet.ObjectIdentifier, error)
	ReadProperties(
		context.Context,
		Route,
		[]service.ReadAccessSpecification,
	) ([]service.ReadAccessResult, error)
	WriteProperty(
		context.Context,
		Route,
		bacnet.ObjectIdentifier,
		bacnet.PropertyReference,
		bacnet.ApplicationValue,
		*uint8,
	) error
	Status() AttachmentSnapshot
	Close() error
}

// Factory creates one client per physical attachment.
type Factory func(config.Attachment) (Client, error)
