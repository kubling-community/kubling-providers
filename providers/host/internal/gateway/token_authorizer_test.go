package gateway

import (
	"context"
	"testing"

	"github.com/kubling-community/kubling-providers/providers/host/internal/gatewayauth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const testBearerToken = "0123456789abcdef0123456789abcdef"

func TestTokenAuthorizerAuthenticatesTLSBearerCredential(t *testing.T) {
	authorizer, err := NewTokenAuthorizer(map[string]string{"fleet-a": testBearerToken})
	if err != nil {
		t.Fatalf("NewTokenAuthorizer() error = %v", err)
	}
	ctx := tokenAuthorizationContext(testBearerToken, true)
	namespace, err := authorizer.Authorize(ctx, "fleet-a")
	if err != nil || namespace != "fleet-a" {
		t.Fatalf("Authorize() = %q, %v", namespace, err)
	}
}

func TestTokenAuthorizerRejectsInvalidCredentialsWithoutNamespaceDisclosure(t *testing.T) {
	authorizer, err := NewTokenAuthorizer(map[string]string{"fleet-a": testBearerToken})
	if err != nil {
		t.Fatalf("NewTokenAuthorizer() error = %v", err)
	}
	tests := map[string]struct {
		ctx       context.Context
		namespace string
	}{
		"plaintext":         {ctx: tokenAuthorizationContext(testBearerToken, false), namespace: "fleet-a"},
		"missing token":     {ctx: tokenAuthorizationContext("", true), namespace: "fleet-a"},
		"wrong token":       {ctx: tokenAuthorizationContext("fedcba9876543210fedcba9876543210", true), namespace: "fleet-a"},
		"unknown namespace": {ctx: tokenAuthorizationContext(testBearerToken, true), namespace: "fleet-b"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := authorizer.Authorize(test.ctx, test.namespace); status.Code(err) != codes.Unauthenticated {
				t.Fatalf("Authorize() code = %v, want Unauthenticated", status.Code(err))
			}
		})
	}
}

func TestNewTokenAuthorizerRejectsWeakCredential(t *testing.T) {
	if _, err := NewTokenAuthorizer(map[string]string{"fleet-a": "short"}); err == nil {
		t.Fatal("NewTokenAuthorizer() error = nil")
	}
}

func TestNewTokenAuthorizerRejectsCredentialSharedAcrossNamespaces(t *testing.T) {
	if _, err := NewTokenAuthorizer(map[string]string{
		"fleet-a": testBearerToken,
		"fleet-b": testBearerToken,
	}); err == nil {
		t.Fatal("NewTokenAuthorizer() error = nil")
	}
}

func tokenAuthorizationContext(token string, secure bool) context.Context {
	ctx := context.Background()
	if token != "" {
		ctx = metadata.NewIncomingContext(
			ctx,
			metadata.Pairs(gatewayauth.MetadataKey, gatewayauth.BearerValue(token)),
		)
	}
	if secure {
		ctx = peer.NewContext(ctx, &peer.Peer{AuthInfo: credentials.TLSInfo{}})
	}
	return ctx
}
