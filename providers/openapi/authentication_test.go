package openapi

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
)

func TestQueryAppliesOpenAPISecurityScheme(t *testing.T) {
	tests := []struct {
		name           string
		authentication AuthenticationConfig
		assertRequest  func(*testing.T, *http.Request)
	}{
		{
			name: "bearer",
			authentication: AuthenticationConfig{
				SecurityScheme: "BearerAuth",
				Credential:     "bearer-token",
			},
			assertRequest: func(t *testing.T, request *http.Request) {
				if request.Header.Get("Authorization") != "Bearer bearer-token" {
					t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
				}
			},
		},
		{
			name: "basic",
			authentication: AuthenticationConfig{
				SecurityScheme: "BasicAuth",
				Username:       "kubling",
				Password:       "secret",
			},
			assertRequest: func(t *testing.T, request *http.Request) {
				username, password, ok := request.BasicAuth()
				if !ok || username != "kubling" || password != "secret" {
					t.Fatalf("BasicAuth() = (%q, %q, %v)", username, password, ok)
				}
			},
		},
		{
			name: "API key header",
			authentication: AuthenticationConfig{
				SecurityScheme: "HeaderKey",
				Credential:     "header-key",
			},
			assertRequest: func(t *testing.T, request *http.Request) {
				if request.Header.Get("X-API-Key") != "header-key" {
					t.Fatalf("X-API-Key = %q", request.Header.Get("X-API-Key"))
				}
			},
		},
		{
			name: "API key query",
			authentication: AuthenticationConfig{
				SecurityScheme: "QueryKey",
				Credential:     "query-key",
			},
			assertRequest: func(t *testing.T, request *http.Request) {
				if request.URL.Query().Get("api_key") != "query-key" {
					t.Fatalf("api_key = %q", request.URL.Query().Get("api_key"))
				}
			},
		},
		{
			name: "API key cookie",
			authentication: AuthenticationConfig{
				SecurityScheme: "CookieKey",
				Credential:     "cookie-key",
			},
			assertRequest: func(t *testing.T, request *http.Request) {
				cookie, err := request.Cookie("session_key")
				if err != nil || cookie.Value != "cookie-key" {
					t.Fatalf("session_key cookie = %#v, %v", cookie, err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
				test.assertRequest(t, request)
				return jsonResponse(http.StatusOK, invoicePage("inv-1")), nil
			})
			connection := newAuthenticationTestConnection(t, client, test.authentication, nil, nil)
			stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
				Entity: &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
			})
			if err != nil {
				t.Fatalf("Query() error = %v", err)
			}
			if _, err := stream.Next(context.Background()); err != nil {
				t.Fatalf("Next() error = %v", err)
			}
		})
	}
}

func TestNewRejectsInvalidOpenAPIAuthentication(t *testing.T) {
	tests := []struct {
		name           string
		authentication AuthenticationConfig
		headers        map[string]string
		entity         *EntityConfig
		want           string
	}{
		{
			name:           "unknown scheme",
			authentication: AuthenticationConfig{SecurityScheme: "Missing", Credential: "secret"},
			want:           `securityScheme "Missing" was not found`,
		},
		{
			name:           "unsupported OAuth",
			authentication: AuthenticationConfig{SecurityScheme: "OAuth", Credential: "secret"},
			want:           `security scheme type "oauth2" is not implemented`,
		},
		{
			name:           "Authorization conflict",
			authentication: AuthenticationConfig{SecurityScheme: "BearerAuth", Credential: "secret"},
			headers:        map[string]string{"authorization": "custom"},
			want:           "conflicts with static Authorization header",
		},
		{
			name:           "bearer newline",
			authentication: AuthenticationConfig{SecurityScheme: "BearerAuth", Credential: "secret\r\ninjected"},
			want:           "credential contains a newline",
		},
		{
			name: "basic username colon",
			authentication: AuthenticationConfig{
				SecurityScheme: "BasicAuth",
				Username:       "user:name",
				Password:       "secret",
			},
			want: "username must not contain a colon",
		},
		{
			name:           "pagination query conflict",
			authentication: AuthenticationConfig{SecurityScheme: "QueryKey", Credential: "secret"},
			entity: &EntityConfig{
				Name:          "INVOICE",
				ListOperation: "listInvoices",
				ResponsePath:  "/data/items",
				Pagination: &PaginationConfig{
					Mode:              PaginationModeOffset,
					PageSize:          10,
					PageSizeParameter: "api_key",
					OffsetParameter:   "offset",
				},
			},
			want: "conflicts with pagination",
		},
		{
			name:           "static query conflict",
			authentication: AuthenticationConfig{SecurityScheme: "QueryKey", Credential: "secret"},
			entity: &EntityConfig{
				Name:            "INVOICE",
				ListOperation:   "listInvoices",
				ResponsePath:    "/data/items",
				QueryParameters: []QueryParameterConfig{{Name: "api_key", Value: "static"}},
			},
			want: "conflicts with a static binding",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newAuthenticationTestProvider(t, nil, test.authentication, test.headers, test.entity)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("New() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestAuthenticationCredentialsAreNotExposedInMetadata(t *testing.T) {
	provider, err := newAuthenticationTestProvider(t, nil, AuthenticationConfig{
		SecurityScheme: "BearerAuth",
		Credential:     "metadata-secret",
	}, nil, nil)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if strings.Contains(metadata.String(), "metadata-secret") {
		t.Fatal("Metadata() exposed authentication credential")
	}
}

func newAuthenticationTestConnection(
	t *testing.T,
	client *http.Client,
	authentication AuthenticationConfig,
	headers map[string]string,
	entity *EntityConfig,
) *Connection {
	t.Helper()
	provider, err := newAuthenticationTestProvider(t, client, authentication, headers, entity)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	connection, err := provider.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return connection.(*Connection)
}

func newAuthenticationTestProvider(
	t *testing.T,
	client *http.Client,
	authentication AuthenticationConfig,
	headers map[string]string,
	entity *EntityConfig,
) (*Provider, error) {
	t.Helper()
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	writeTestFile(t, specPath, testOpenAPISpec)
	if entity == nil {
		entity = &EntityConfig{
			Name:          "INVOICE",
			ListOperation: "listInvoices",
			ResponsePath:  "/data/items",
		}
	}
	return New(Config{
		SpecFile:       specPath,
		BaseURL:        "https://billing.example.test",
		Namespace:      "billing",
		Headers:        headers,
		Authentication: &authentication,
		HTTPClient:     client,
		Entities:       []EntityConfig{*entity},
	})
}
