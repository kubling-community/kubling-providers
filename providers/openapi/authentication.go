package openapi

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
)

type authenticationKind uint8

const (
	authenticationBearer authenticationKind = iota + 1
	authenticationBasic
	authenticationAPIKeyHeader
	authenticationAPIKeyQuery
	authenticationAPIKeyCookie
)

type requestAuthentication struct {
	kind       authenticationKind
	name       string
	credential string
	username   string
	password   string
}

func resolveAuthentication(config Config, document *v3.Document) (*requestAuthentication, error) {
	if config.Authentication == nil {
		return nil, nil
	}
	if document == nil || document.Components == nil || document.Components.SecuritySchemes == nil {
		return nil, fmt.Errorf(
			"authentication securityScheme %q was not found in the OpenAPI document",
			config.Authentication.SecurityScheme,
		)
	}
	scheme := document.Components.SecuritySchemes.GetOrZero(config.Authentication.SecurityScheme)
	if scheme == nil {
		return nil, fmt.Errorf(
			"authentication securityScheme %q was not found in the OpenAPI document",
			config.Authentication.SecurityScheme,
		)
	}

	authentication, err := authenticationForScheme(config.Authentication, scheme)
	if err != nil {
		return nil, fmt.Errorf("authentication securityScheme %q: %w", config.Authentication.SecurityScheme, err)
	}
	if err := validateAuthenticationConflicts(config, authentication); err != nil {
		return nil, err
	}
	return authentication, nil
}

func authenticationForScheme(
	config *AuthenticationConfig,
	scheme *v3.SecurityScheme,
) (*requestAuthentication, error) {
	switch strings.ToLower(strings.TrimSpace(scheme.Type)) {
	case "http":
		switch strings.ToLower(strings.TrimSpace(scheme.Scheme)) {
		case "bearer":
			if strings.TrimSpace(config.Credential) == "" {
				return nil, errors.New("credential is required for HTTP bearer authentication")
			}
			if strings.ContainsAny(config.Credential, "\r\n") {
				return nil, errors.New("credential contains a newline")
			}
			if config.Username != "" || config.Password != "" {
				return nil, errors.New("username and password are not valid for HTTP bearer authentication")
			}
			return &requestAuthentication{
				kind:       authenticationBearer,
				credential: config.Credential,
			}, nil
		case "basic":
			if config.Username == "" {
				return nil, errors.New("username is required for HTTP basic authentication")
			}
			if strings.Contains(config.Username, ":") {
				return nil, errors.New("username must not contain a colon for HTTP basic authentication")
			}
			if config.Credential != "" {
				return nil, errors.New("credential is not valid for HTTP basic authentication")
			}
			return &requestAuthentication{
				kind:     authenticationBasic,
				username: config.Username,
				password: config.Password,
			}, nil
		default:
			return nil, fmt.Errorf("HTTP scheme %q is not supported", scheme.Scheme)
		}
	case "apikey":
		if strings.TrimSpace(config.Credential) == "" {
			return nil, errors.New("credential is required for API key authentication")
		}
		if config.Username != "" || config.Password != "" {
			return nil, errors.New("username and password are not valid for API key authentication")
		}
		if strings.TrimSpace(scheme.Name) == "" {
			return nil, errors.New("OpenAPI API key name is required")
		}
		kind := authenticationAPIKeyHeader
		switch strings.ToLower(strings.TrimSpace(scheme.In)) {
		case "header":
			if !validHTTPToken(scheme.Name) {
				return nil, fmt.Errorf("invalid API key header name %q", scheme.Name)
			}
			if strings.ContainsAny(config.Credential, "\r\n") {
				return nil, errors.New("credential contains a newline")
			}
		case "query":
			kind = authenticationAPIKeyQuery
		case "cookie":
			kind = authenticationAPIKeyCookie
			if err := (&http.Cookie{Name: scheme.Name, Value: config.Credential}).Valid(); err != nil {
				return nil, fmt.Errorf("invalid API key cookie: %w", err)
			}
		default:
			return nil, fmt.Errorf("API key location %q is not supported", scheme.In)
		}
		return &requestAuthentication{
			kind:       kind,
			name:       scheme.Name,
			credential: config.Credential,
		}, nil
	case "oauth2", "openidconnect", "mutualtls":
		return nil, fmt.Errorf("security scheme type %q is not implemented", scheme.Type)
	default:
		return nil, fmt.Errorf("security scheme type %q is not supported", scheme.Type)
	}
}

func validateAuthenticationConflicts(config Config, authentication *requestAuthentication) error {
	switch authentication.kind {
	case authenticationBearer, authenticationBasic:
		if hasHeader(config.Headers, "Authorization") {
			return errors.New("authentication conflicts with static Authorization header")
		}
	case authenticationAPIKeyHeader:
		if hasHeader(config.Headers, authentication.name) {
			return fmt.Errorf("authentication conflicts with static %s header", authentication.name)
		}
	case authenticationAPIKeyCookie:
		if hasHeader(config.Headers, "Cookie") {
			return errors.New("cookie authentication conflicts with static Cookie header")
		}
	case authenticationAPIKeyQuery:
		for _, entity := range config.Entities {
			for _, parameter := range entity.QueryParameters {
				if parameter.Name == authentication.name {
					return fmt.Errorf(
						"authentication query parameter %q conflicts with a static binding for entity %q",
						authentication.name,
						entity.Name,
					)
				}
			}
			for _, filter := range entity.EqualityFilters {
				if filter.Parameter == authentication.name {
					return fmt.Errorf(
						"authentication query parameter %q conflicts with an equality filter for entity %q",
						authentication.name,
						entity.Name,
					)
				}
			}
			if paginationUsesParameter(entity.Pagination, authentication.name) {
				return fmt.Errorf(
					"authentication query parameter %q conflicts with pagination for entity %q",
					authentication.name,
					entity.Name,
				)
			}
		}
	}
	return nil
}

func hasHeader(headers map[string]string, name string) bool {
	for candidate := range headers {
		if strings.EqualFold(candidate, name) {
			return true
		}
	}
	return false
}

func paginationUsesParameter(pagination *PaginationConfig, name string) bool {
	if pagination == nil {
		return false
	}
	if pagination.PageSizeParameter == name {
		return true
	}
	switch pagination.Mode {
	case PaginationModeOffset:
		return pagination.OffsetParameter == name
	case PaginationModePage:
		return pagination.PageParameter == name
	case PaginationModeCursor:
		return pagination.CursorParameter == name
	default:
		return false
	}
}

func (authentication *requestAuthentication) apply(request *http.Request) {
	if authentication == nil {
		return
	}
	switch authentication.kind {
	case authenticationBearer:
		request.Header.Set("Authorization", "Bearer "+authentication.credential)
	case authenticationBasic:
		request.SetBasicAuth(authentication.username, authentication.password)
	case authenticationAPIKeyHeader:
		request.Header.Set(authentication.name, authentication.credential)
	case authenticationAPIKeyQuery:
		query := request.URL.Query()
		query.Set(authentication.name, authentication.credential)
		request.URL.RawQuery = query.Encode()
	case authenticationAPIKeyCookie:
		request.AddCookie(&http.Cookie{Name: authentication.name, Value: authentication.credential})
	}
}
