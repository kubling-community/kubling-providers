package host

import (
	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
)

func baseMetadata() *providersdk.Metadata {
	return hostschema.Metadata()
}
