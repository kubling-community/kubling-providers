package gateway

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"strings"

	"github.com/kubling-community/kubling-providers/providers/host/internal/gatewayauth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// TokenAuthorizer authenticates an agent bearer token and maps it to exactly
// one namespace. Raw tokens are hashed at construction and never retained.
type TokenAuthorizer struct {
	tokenHashes map[string][sha256.Size]byte
}

// NewTokenAuthorizer creates a TLS-bound namespace authorizer from raw bearer
// tokens loaded by the provider process.
func NewTokenAuthorizer(tokens map[string]string) (*TokenAuthorizer, error) {
	if len(tokens) == 0 {
		return nil, errors.New("at least one agent namespace credential is required")
	}
	hashes := make(map[string][sha256.Size]byte, len(tokens))
	namespacesByHash := make(map[[sha256.Size]byte]string, len(tokens))
	for namespace, token := range tokens {
		if strings.TrimSpace(namespace) == "" || strings.TrimSpace(namespace) != namespace {
			return nil, errors.New("agent credential namespace is invalid")
		}
		if err := gatewayauth.ValidateToken(token); err != nil {
			return nil, err
		}
		hash := sha256.Sum256([]byte(token))
		if _, exists := namespacesByHash[hash]; exists {
			return nil, errors.New("agent bearer tokens must be unique across namespaces")
		}
		hashes[namespace] = hash
		namespacesByHash[hash] = namespace
	}
	return &TokenAuthorizer{tokenHashes: hashes}, nil
}

// Authorize requires TLS and a valid Authorization: Bearer credential for the
// requested namespace. Authentication failures do not disclose configured
// namespaces.
func (a *TokenAuthorizer) Authorize(
	ctx context.Context,
	requestedNamespace string,
) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", status.FromContextError(err).Err()
	}
	connectionPeer, ok := peer.FromContext(ctx)
	if !ok || connectionPeer.AuthInfo == nil {
		return "", status.Error(codes.Unauthenticated, "secure Agent Gateway transport is required")
	}
	if _, ok := connectionPeer.AuthInfo.(credentials.TLSInfo); !ok {
		return "", status.Error(codes.Unauthenticated, "secure Agent Gateway transport is required")
	}
	metadataValues, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", invalidAgentCredential()
	}
	values := metadataValues.Get(gatewayauth.MetadataKey)
	if len(values) != 1 {
		return "", invalidAgentCredential()
	}
	token, ok := gatewayauth.ParseBearerValue(values[0])
	if !ok {
		return "", invalidAgentCredential()
	}
	expected, exists := a.tokenHashes[requestedNamespace]
	provided := sha256.Sum256([]byte(token))
	if !exists || subtle.ConstantTimeCompare(provided[:], expected[:]) != 1 {
		return "", invalidAgentCredential()
	}
	return requestedNamespace, nil
}

func invalidAgentCredential() error {
	return status.Error(codes.Unauthenticated, "agent credentials are invalid")
}

var _ Authorizer = (*TokenAuthorizer)(nil)
