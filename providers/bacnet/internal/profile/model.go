// Package profile loads and compiles declarative BACnet device profiles.
package profile

import kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"

const (
	// APIVersion identifies the only profile document format accepted by this
	// provider version.
	APIVersion = "kubling.io/bacnet-profile/v1"

	// StandardProfileID is the built-in fallback profile.
	StandardProfileID = "bacnet-standard"
)

const (
	maximumDeviceInstance        = uint32(4_194_303)
	maximumVendorIdentifier      = uint32(65_535)
	maximumObjectType            = uint32(1_023)
	maximumPropertyID            = uint32(4_194_303)
	minimumProprietaryObjectType = uint32(128)
)

type document struct {
	APIVersion  string               `yaml:"apiVersion"`
	ID          string               `yaml:"id"`
	Abstract    bool                 `yaml:"abstract"`
	Extends     []string             `yaml:"extends"`
	Selectors   []selectorDocument   `yaml:"selectors"`
	ObjectTypes []objectTypeDocument `yaml:"objectTypes"`
	Tables      []tableDocument      `yaml:"tables"`
	source      string
}

type selectorDocument struct {
	VendorIdentifier *uint32 `yaml:"vendorIdentifier"`
	ModelName        string  `yaml:"modelName"`
	FirmwareRevision string  `yaml:"firmwareRevision"`
	DeviceInstance   *uint32 `yaml:"deviceInstance"`
	Namespace        string  `yaml:"namespace"`
	ScopeID          string  `yaml:"scopeId"`
}

type objectTypeDocument struct {
	ID   uint32 `yaml:"id"`
	Name string `yaml:"name"`
}

type tableDocument struct {
	Name           string           `yaml:"name"`
	AllObjectTypes bool             `yaml:"allObjectTypes"`
	ObjectTypes    []string         `yaml:"objectTypes"`
	Columns        []columnDocument `yaml:"columns"`
}

type columnDocument struct {
	Name        string            `yaml:"name"`
	Property    propertyDocument  `yaml:"property"`
	BACnetType  BACnetType        `yaml:"bacnetType"`
	KublingType string            `yaml:"kublingType"`
	Nullable    *bool             `yaml:"nullable"`
	Required    bool              `yaml:"required"`
	Array       bool              `yaml:"array"`
	Enum        map[uint32]string `yaml:"enum"`
	Write       *writeDocument    `yaml:"write"`
	Override    bool              `yaml:"override"`
}

type writeDocument struct {
	ObjectTypes []string `yaml:"objectTypes"`
	Commandable bool     `yaml:"commandable"`
}

type propertyDocument struct {
	ID   uint32 `yaml:"id"`
	Name string `yaml:"name"`
}

// BACnetType is the expected BACnet application value type for a property.
type BACnetType string

const (
	BACnetBoolean          BACnetType = "boolean"
	BACnetUnsigned         BACnetType = "unsigned"
	BACnetSigned           BACnetType = "signed"
	BACnetReal             BACnetType = "real"
	BACnetDouble           BACnetType = "double"
	BACnetOctetString      BACnetType = "octet-string"
	BACnetCharacterString  BACnetType = "character-string"
	BACnetBitString        BACnetType = "bit-string"
	BACnetEnumerated       BACnetType = "enumerated"
	BACnetDate             BACnetType = "date"
	BACnetTime             BACnetType = "time"
	BACnetObjectIdentifier BACnetType = "object-identifier"
)

// Catalog is the deterministic set of compiled profiles available to the
// provider.
type Catalog struct {
	profiles map[string]*Profile
	ordered  []*Profile
	fallback *Profile
}

// Profiles returns profiles in stable identifier order.
func (c *Catalog) Profiles() []*Profile {
	if c == nil {
		return nil
	}
	return append([]*Profile(nil), c.ordered...)
}

// Profile returns a compiled profile by identifier.
func (c *Catalog) Profile(id string) (*Profile, bool) {
	if c == nil {
		return nil, false
	}
	result, ok := c.profiles[id]
	return result, ok
}

// Profile is one fully inherited, validated device profile.
type Profile struct {
	ID          string
	Abstract    bool
	Extends     []string
	Selectors   []Selector
	ObjectTypes []ObjectType
	Tables      []Table
}

// Selector matches an exact BACnet device identity. Zero-value fields are not
// constraints.
type Selector struct {
	VendorIdentifier *uint32
	ModelName        string
	FirmwareRevision string
	DeviceInstance   *uint32
	Namespace        string
	ScopeID          string
}

// DeviceIdentity contains the stable identification properties used to select
// a compiled profile.
type DeviceIdentity struct {
	VendorIdentifier uint32
	ModelName        string
	FirmwareRevision string
	DeviceInstance   uint32
	Namespace        string
	ScopeID          string
}

// ObjectType names one standard or proprietary BACnet object type.
type ObjectType struct {
	ID             uint32
	Name           string
	SourceProfile  string
	SourceProfiles []string
}

// Table is one relational projection declared by a compiled profile.
type Table struct {
	Name           string
	AllObjectTypes bool
	ObjectTypes    []ObjectType
	Columns        []Column
	SourceProfiles []string
}

// Column binds one logical column to a BACnet property.
type Column struct {
	Name           string
	Property       Property
	BACnetType     BACnetType
	Type           *kublingv1.TypeDescriptor
	Nullable       bool
	Required       bool
	Enum           map[uint32]string
	Write          *WritePolicy
	SourceProfile  string
	SourceProfiles []string
}

// WritePolicy declares the object types for which a profile allows writing a
// BACnet property. Commandable properties use the scope's BACnet priority and
// accept NULL as a relinquish command.
type WritePolicy struct {
	ObjectTypes []string
	Commandable bool
}

// WritableFor reports whether this column may be written on the named object
// type according to the selected device profile.
func (c Column) WritableFor(objectType string) bool {
	if c.Write == nil {
		return false
	}
	for _, candidate := range c.Write.ObjectTypes {
		if candidate == objectType {
			return true
		}
	}
	return false
}

// Property identifies one standard or proprietary BACnet property.
type Property struct {
	ID   uint32
	Name string
}
