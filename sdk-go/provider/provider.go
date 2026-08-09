package provider

import (
	"context"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

// Capabilities describes the global capabilities exposed by a provider server.
type Capabilities = providerv1.GetCapabilitiesResponse

// Metadata describes provider-neutral relational schema metadata.
type Metadata = providerv1.SchemaMetadata

// Provider is implemented by a Kubling provider.
//
// The SDK owns the gRPC transport and the lifecycle of connection identifiers.
type Provider interface {
	// Capabilities returns the capabilities exposed by this provider server.
	Capabilities(context.Context) (*Capabilities, error)

	// Health returns the connection-agnostic health state of the provider.
	Health(context.Context) (*providerv1.HealthResponse, error)

	// Open creates a logical connection to the provider's data universe.
	//
	// The SDK assigns and manages the public connection identifier.
	Open(context.Context) (Connection, error)
}

// SchemaProvider may be implemented by a provider that exposes its own Kubling
// schema definition.
//
// When this interface is not implemented, GetSchema returns an empty schema and
// Kubling must obtain the DDL from the data source configuration.
type SchemaProvider interface {
	// Schema returns the Kubling DDL exposed by this provider server.
	Schema(context.Context) (string, error)
}

// MetadataProvider may be implemented by a provider that discovers relational
// metadata dynamically.
//
// Returning nil metadata delegates schema resolution to the DDL interfaces.
type MetadataProvider interface {
	// Metadata returns provider-neutral relational schema metadata.
	Metadata(context.Context) (*Metadata, error)
}
