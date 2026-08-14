package openapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
	"google.golang.org/protobuf/proto"
)

type Provider struct {
	config         Config
	catalog        *catalog
	client         *http.Client
	authentication *requestAuthentication
}

func New(config Config) (*Provider, error) {
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	client := normalized.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	document, err := loadDocument(normalized.SpecFile, normalized.SpecHeaders, client, normalized.RequestTimeout)
	if err != nil {
		return nil, err
	}
	normalized.BaseURL, err = resolveBaseURL(normalized.BaseURL, document)
	if err != nil {
		return nil, err
	}
	if err := validateDatasourceTransport(normalized); err != nil {
		return nil, err
	}
	datasourceOrigin, err := url.Parse(normalized.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse OpenAPI datasource base URL: %w", err)
	}
	client = sameOriginHTTPClient(client, datasourceOrigin, "OpenAPI datasource")
	authentication, err := resolveAuthentication(normalized, document)
	if err != nil {
		return nil, err
	}
	catalog, err := buildCatalog(normalized, document)
	if err != nil {
		return nil, err
	}
	return &Provider{
		config:         normalized,
		catalog:        catalog,
		client:         client,
		authentication: authentication,
	}, nil
}

func validateDatasourceTransport(config Config) error {
	parsed, err := url.Parse(config.BaseURL)
	if err != nil {
		return fmt.Errorf("parse OpenAPI datasource base URL: %w", err)
	}
	if strings.EqualFold(parsed.Scheme, "http") && !config.AllowInsecureHTTP &&
		(config.Authentication != nil || len(config.Headers) > 0) {
		return errors.New("allowInsecureHttp must be true to send authentication or headers over HTTP")
	}
	return nil
}

func resolveBaseURL(configured string, document *v3.Document) (string, error) {
	if configured != "" {
		return configured, nil
	}
	if document == nil || len(document.Servers) == 0 || document.Servers[0] == nil {
		return "", errors.New("baseUrl is required when the OpenAPI document defines no server")
	}
	serverURL := strings.TrimRight(strings.TrimSpace(document.Servers[0].URL), "/")
	if strings.ContainsAny(serverURL, "{}") {
		return "", errors.New("baseUrl is required when the first OpenAPI server contains variables")
	}
	if err := validateBaseURL(serverURL); err != nil {
		return "", fmt.Errorf("first OpenAPI server URL: %w", err)
	}
	return serverURL, nil
}

func (p *Provider) Capabilities(context.Context) (*providersdk.Capabilities, error) {
	mutations := &providerv1.MutationCapabilities{}
	for _, descriptor := range p.catalog.entities {
		mutations.Insert = mutations.Insert || descriptor.insert != nil
		mutations.Update = mutations.Update || descriptor.update != nil
		mutations.Delete = mutations.Delete || descriptor.delete != nil
	}
	return &providersdk.Capabilities{
		Transactions: &providerv1.TransactionCapabilities{Supported: false},
		Query: &providerv1.QueryCapabilities{
			Ordering:   &providerv1.OrderingCapabilities{Supported: false},
			Pagination: &providerv1.PaginationCapabilities{Limit: true, Offset: true},
			Expressions: &providerv1.ExpressionCapabilities{
				ComparisonOperators: []providerv1.ComparisonOperator{
					providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
				},
				LogicalOperators: []providerv1.LogicalOperator{
					providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
				},
			},
		},
		Mutations: mutations,
	}, nil
}

func (*Provider) Health(context.Context) (*providerv1.HealthResponse, error) {
	return &providerv1.HealthResponse{
		Healthy: true,
		Message: "OpenAPI specification and entity mappings loaded",
	}, nil
}

func (p *Provider) Open(context.Context) (providersdk.Connection, error) {
	return &Connection{provider: p}, nil
}

func (p *Provider) Metadata(context.Context) (*providersdk.Metadata, error) {
	return proto.Clone(p.catalog.metadata).(*providerv1.SchemaMetadata), nil
}

var _ providersdk.Provider = (*Provider)(nil)
var _ providersdk.MetadataProvider = (*Provider)(nil)
