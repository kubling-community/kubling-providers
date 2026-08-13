package openapi

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	defaultMaxResponseBytes int64 = 32 << 20
	maxResponseBytesLimit   int64 = 1 << 30
)

type Config struct {
	SpecFile          string
	SpecHeaders       map[string]string
	BaseURL           string
	Namespace         string
	RequestTimeout    time.Duration
	MaxResponseBytes  int64
	AllowInsecureHTTP bool
	Headers           map[string]string
	Authentication    *AuthenticationConfig
	HTTPClient        *http.Client
	Discovery         *DiscoveryConfig
	Entities          []EntityConfig
}

type DiscoveryConfig struct {
	Enabled           bool
	IncludeTags       []string
	IncludeOperations []string
	ExcludeOperations []string
}

type AuthenticationConfig struct {
	SecurityScheme string
	Credential     string
	Username       string
	Password       string
}

type EntityConfig struct {
	Name            string
	ListOperation   string
	ResponsePath    string
	PrimaryKey      []string
	QueryParameters []QueryParameterConfig
	EqualityFilters []EqualityFilterConfig
	Pagination      *PaginationConfig
	Mutations       *EntityMutationConfig
}

type EntityMutationConfig struct {
	Insert *MutationOperationConfig
	Update *MutationOperationConfig
	Delete *MutationOperationConfig
}

type MutationOperationConfig struct {
	Operation       string
	PathParameters  []PathParameterConfig
	QueryParameters []QueryParameterConfig
	BodyPath        string
}

type PathParameterConfig struct {
	Parameter string
	Field     string
	Value     string
}

type QueryParameterConfig struct {
	Name  string
	Value string
}

type EqualityFilterConfig struct {
	Field     string
	Parameter string
}

type PaginationMode string

const (
	PaginationModeOffset PaginationMode = "OFFSET"
	PaginationModePage   PaginationMode = "PAGE"
	PaginationModeCursor PaginationMode = "CURSOR"
)

type PaginationConfig struct {
	Mode              PaginationMode
	PageSize          uint32
	PageSizeParameter string
	OffsetParameter   string
	PageParameter     string
	StartPage         *uint64
	CursorParameter   string
	NextCursorPath    string
	HasMorePath       string
	MaxPages          uint32
}

type fileConfig struct {
	SpecFile          string                    `yaml:"specFile"`
	SpecHeaders       map[string]string         `yaml:"specHeaders"`
	BaseURL           string                    `yaml:"baseUrl"`
	Namespace         string                    `yaml:"namespace"`
	RequestTimeout    string                    `yaml:"requestTimeout"`
	MaxResponseBytes  int64                     `yaml:"maxResponseBytes"`
	AllowInsecureHTTP bool                      `yaml:"allowInsecureHttp"`
	Headers           map[string]string         `yaml:"headers"`
	Authentication    *fileAuthenticationConfig `yaml:"authentication"`
	Discovery         *fileDiscoveryConfig      `yaml:"discovery"`
	Entities          []fileEntityConfig        `yaml:"entities"`
}

type fileDiscoveryConfig struct {
	Enabled           bool     `yaml:"enabled"`
	IncludeTags       []string `yaml:"includeTags"`
	IncludeOperations []string `yaml:"includeOperations"`
	ExcludeOperations []string `yaml:"excludeOperations"`
}

type fileAuthenticationConfig struct {
	SecurityScheme string `yaml:"securityScheme"`
	Credential     string `yaml:"credential"`
	Username       string `yaml:"username"`
	Password       string `yaml:"password"`
}

type fileEntityConfig struct {
	Name            string                     `yaml:"name"`
	ListOperation   string                     `yaml:"listOperation"`
	ResponsePath    string                     `yaml:"responsePath"`
	PrimaryKey      []string                   `yaml:"primaryKey"`
	QueryParameters []fileQueryParameterConfig `yaml:"queryParameters"`
	EqualityFilters []fileEqualityFilterConfig `yaml:"equalityFilters"`
	Pagination      *filePaginationConfig      `yaml:"pagination"`
	Mutations       *fileEntityMutationConfig  `yaml:"mutations"`
}

type fileEntityMutationConfig struct {
	Insert *fileMutationOperationConfig `yaml:"insert"`
	Update *fileMutationOperationConfig `yaml:"update"`
	Delete *fileMutationOperationConfig `yaml:"delete"`
}

type fileMutationOperationConfig struct {
	Operation       string                     `yaml:"operation"`
	PathParameters  []filePathParameterConfig  `yaml:"pathParameters"`
	QueryParameters []fileQueryParameterConfig `yaml:"queryParameters"`
	BodyPath        string                     `yaml:"bodyPath"`
}

type filePathParameterConfig struct {
	Parameter string `yaml:"parameter"`
	Field     string `yaml:"field"`
	Value     string `yaml:"value"`
}

type fileQueryParameterConfig struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type fileEqualityFilterConfig struct {
	Field     string `yaml:"field"`
	Parameter string `yaml:"parameter"`
}

type filePaginationConfig struct {
	Mode              string  `yaml:"mode"`
	PageSize          uint32  `yaml:"pageSize"`
	PageSizeParameter string  `yaml:"pageSizeParameter"`
	OffsetParameter   string  `yaml:"offsetParameter"`
	PageParameter     string  `yaml:"pageParameter"`
	StartPage         *uint64 `yaml:"startPage"`
	CursorParameter   string  `yaml:"cursorParameter"`
	NextCursorPath    string  `yaml:"nextCursorPath"`
	HasMorePath       string  `yaml:"hasMorePath"`
	MaxPages          uint32  `yaml:"maxPages"`
}

func LoadConfig(path string) (Config, error) {
	return loadConfig(path, true)
}

func loadConfig(path string, requireEntities bool) (Config, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read OpenAPI provider config: %w", err)
	}
	contents, err = expandEnvironment(contents, os.LookupEnv)
	if err != nil {
		return Config{}, fmt.Errorf("expand OpenAPI provider config: %w", err)
	}

	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	decoder.KnownFields(true)

	var serialized fileConfig
	if err := decoder.Decode(&serialized); err != nil {
		return Config{}, fmt.Errorf("decode OpenAPI provider config: %w", err)
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, errors.New("decode OpenAPI provider config: multiple YAML documents are not supported")
		}
		return Config{}, fmt.Errorf("decode OpenAPI provider config: %w", err)
	}

	specFile := serialized.SpecFile
	if specFile != "" && !isRemoteSpecification(specFile) && !filepath.IsAbs(specFile) {
		specFile = filepath.Join(filepath.Dir(path), specFile)
	}

	entities := make([]EntityConfig, len(serialized.Entities))
	for index, entity := range serialized.Entities {
		entities[index] = EntityConfig{
			Name:            entity.Name,
			ListOperation:   entity.ListOperation,
			ResponsePath:    entity.ResponsePath,
			PrimaryKey:      append([]string(nil), entity.PrimaryKey...),
			QueryParameters: entity.queryParameters(),
			EqualityFilters: entity.equalityFilters(),
			Pagination:      entity.Pagination.toConfig(),
			Mutations:       entity.Mutations.toConfig(),
		}
	}
	requestTimeout, err := parseRequestTimeout(serialized.RequestTimeout)
	if err != nil {
		return Config{}, err
	}

	return normalizeConfigWithEntityRequirement(Config{
		SpecFile:          specFile,
		SpecHeaders:       serialized.SpecHeaders,
		BaseURL:           serialized.BaseURL,
		Namespace:         serialized.Namespace,
		RequestTimeout:    requestTimeout,
		MaxResponseBytes:  serialized.MaxResponseBytes,
		AllowInsecureHTTP: serialized.AllowInsecureHTTP,
		Headers:           serialized.Headers,
		Authentication:    serialized.Authentication.toConfig(),
		Discovery:         serialized.Discovery.toConfig(),
		Entities:          entities,
	}, requireEntities)
}

func expandEnvironment(contents []byte, lookup func(string) (string, bool)) ([]byte, error) {
	var result strings.Builder
	for position := 0; position < len(contents); {
		if contents[position] != '$' {
			result.WriteByte(contents[position])
			position++
			continue
		}
		if position+1 < len(contents) && contents[position+1] == '$' {
			result.WriteByte('$')
			position += 2
			continue
		}
		if position+1 >= len(contents) || contents[position+1] != '{' {
			result.WriteByte(contents[position])
			position++
			continue
		}
		end := bytes.IndexByte(contents[position+2:], '}')
		if end < 0 {
			return nil, errors.New("environment placeholder is missing }")
		}
		end += position + 2
		expression := string(contents[position+2 : end])
		name, fallback, hasFallback := strings.Cut(expression, ":-")
		if !validEnvironmentName(name) {
			return nil, fmt.Errorf("invalid environment variable name %q", name)
		}
		value, exists := lookup(name)
		if (!exists || value == "") && hasFallback {
			value = fallback
			exists = true
		}
		if !exists {
			return nil, fmt.Errorf("environment variable %s is not set", name)
		}
		result.WriteString(value)
		position = end + 1
	}
	return []byte(result.String()), nil
}

func validEnvironmentName(name string) bool {
	if name == "" || name[0] != '_' && (name[0] < 'A' || name[0] > 'Z') && (name[0] < 'a' || name[0] > 'z') {
		return false
	}
	for index := 1; index < len(name); index++ {
		character := name[index]
		if character != '_' && (character < 'A' || character > 'Z') && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func normalizeConfig(config Config) (Config, error) {
	return normalizeConfigWithEntityRequirement(config, true)
}

func normalizeConfigWithEntityRequirement(config Config, requireEntities bool) (Config, error) {
	normalized := Config{
		SpecFile:          strings.TrimSpace(config.SpecFile),
		SpecHeaders:       make(map[string]string, len(config.SpecHeaders)),
		BaseURL:           strings.TrimRight(strings.TrimSpace(config.BaseURL), "/"),
		Namespace:         strings.TrimSpace(config.Namespace),
		RequestTimeout:    config.RequestTimeout,
		MaxResponseBytes:  config.MaxResponseBytes,
		AllowInsecureHTTP: config.AllowInsecureHTTP,
		Headers:           make(map[string]string, len(config.Headers)),
		Authentication:    cloneAuthentication(config.Authentication),
		HTTPClient:        config.HTTPClient,
		Discovery:         cloneDiscovery(config.Discovery),
		Entities:          make([]EntityConfig, len(config.Entities)),
	}
	if normalized.SpecFile == "" {
		return Config{}, errors.New("specFile is required")
	}
	if err := validateSpecificationLocation(normalized.SpecFile); err != nil {
		return Config{}, err
	}
	if normalized.BaseURL != "" {
		if err := validateBaseURL(normalized.BaseURL); err != nil {
			return Config{}, err
		}
	}
	if err := normalizeHeaders(normalized.SpecHeaders, config.SpecHeaders, "specification"); err != nil {
		return Config{}, err
	}
	if len(normalized.SpecHeaders) > 0 {
		parsed, _ := url.Parse(normalized.SpecFile)
		if strings.EqualFold(parsed.Scheme, "http") {
			return Config{}, errors.New("specHeaders require an HTTPS specFile URL")
		}
	}
	if normalized.RequestTimeout < 0 {
		return Config{}, errors.New("requestTimeout must not be negative")
	}
	if normalized.RequestTimeout == 0 {
		normalized.RequestTimeout = 30 * time.Second
	}
	if normalized.MaxResponseBytes < 0 {
		return Config{}, errors.New("maxResponseBytes must not be negative")
	}
	if normalized.MaxResponseBytes > maxResponseBytesLimit {
		return Config{}, fmt.Errorf("maxResponseBytes must not exceed %d", maxResponseBytesLimit)
	}
	if normalized.MaxResponseBytes == 0 {
		normalized.MaxResponseBytes = defaultMaxResponseBytes
	}
	if err := normalizeHeaders(normalized.Headers, config.Headers, "datasource"); err != nil {
		return Config{}, err
	}
	if normalized.Authentication != nil {
		normalized.Authentication.SecurityScheme = strings.TrimSpace(normalized.Authentication.SecurityScheme)
		if normalized.Authentication.SecurityScheme == "" {
			return Config{}, errors.New("authentication.securityScheme is required")
		}
	}
	discovery, err := normalizeDiscovery(normalized.Discovery)
	if err != nil {
		return Config{}, fmt.Errorf("discovery: %w", err)
	}
	normalized.Discovery = discovery
	if requireEntities && len(config.Entities) == 0 && (discovery == nil || !discovery.Enabled) {
		return Config{}, errors.New("at least one OpenAPI entity is required")
	}

	names := make(map[string]struct{}, len(config.Entities))
	for index, candidate := range config.Entities {
		entity, err := normalizeEntity(candidate)
		if err != nil {
			return Config{}, fmt.Errorf("entity %d: %w", index, err)
		}
		lookupName := strings.ToUpper(entity.Name)
		if _, exists := names[lookupName]; exists {
			return Config{}, fmt.Errorf("duplicate entity name %q", entity.Name)
		}
		names[lookupName] = struct{}{}
		normalized.Entities[index] = entity
	}

	return normalized, nil
}

func normalizeHeaders(target, source map[string]string, purpose string) error {
	for rawName, value := range source {
		name := strings.TrimSpace(rawName)
		if !validHTTPToken(name) {
			return fmt.Errorf("invalid %s HTTP header name %q", purpose, rawName)
		}
		if strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("%s HTTP header %q contains a newline", purpose, name)
		}
		target[name] = value
	}
	return nil
}

func (discovery *fileDiscoveryConfig) toConfig() *DiscoveryConfig {
	if discovery == nil {
		return nil
	}
	return &DiscoveryConfig{
		Enabled:           discovery.Enabled,
		IncludeTags:       append([]string(nil), discovery.IncludeTags...),
		IncludeOperations: append([]string(nil), discovery.IncludeOperations...),
		ExcludeOperations: append([]string(nil), discovery.ExcludeOperations...),
	}
}

func cloneDiscovery(discovery *DiscoveryConfig) *DiscoveryConfig {
	if discovery == nil {
		return nil
	}
	return &DiscoveryConfig{
		Enabled:           discovery.Enabled,
		IncludeTags:       append([]string(nil), discovery.IncludeTags...),
		IncludeOperations: append([]string(nil), discovery.IncludeOperations...),
		ExcludeOperations: append([]string(nil), discovery.ExcludeOperations...),
	}
}

func normalizeDiscovery(discovery *DiscoveryConfig) (*DiscoveryConfig, error) {
	if discovery == nil {
		return nil, nil
	}
	normalized := &DiscoveryConfig{Enabled: discovery.Enabled}
	var err error
	normalized.IncludeTags, err = normalizeUniqueStrings(discovery.IncludeTags, "includeTags")
	if err != nil {
		return nil, err
	}
	normalized.IncludeOperations, err = normalizeUniqueStrings(discovery.IncludeOperations, "includeOperations")
	if err != nil {
		return nil, err
	}
	normalized.ExcludeOperations, err = normalizeUniqueStrings(discovery.ExcludeOperations, "excludeOperations")
	if err != nil {
		return nil, err
	}
	excluded := make(map[string]struct{}, len(normalized.ExcludeOperations))
	for _, operation := range normalized.ExcludeOperations {
		excluded[strings.ToUpper(operation)] = struct{}{}
	}
	for _, operation := range normalized.IncludeOperations {
		if _, exists := excluded[strings.ToUpper(operation)]; exists {
			return nil, fmt.Errorf("operation %q is both included and excluded", operation)
		}
	}
	return normalized, nil
}

func normalizeUniqueStrings(values []string, field string) ([]string, error) {
	normalized := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for index, raw := range values {
		value := strings.TrimSpace(raw)
		if value == "" {
			return nil, fmt.Errorf("%s %d must not be blank", field, index)
		}
		lookup := strings.ToUpper(value)
		if _, exists := seen[lookup]; exists {
			return nil, fmt.Errorf("duplicate %s value %q", field, value)
		}
		seen[lookup] = struct{}{}
		normalized = append(normalized, value)
	}
	return normalized, nil
}

func validateSpecificationLocation(location string) error {
	parsed, err := url.Parse(location)
	if err != nil {
		return fmt.Errorf("parse specFile: %w", err)
	}
	if parsed.Scheme == "" {
		return nil
	}
	if !strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https") {
		return fmt.Errorf("specFile URL scheme %q is not supported", parsed.Scheme)
	}
	if parsed.Host == "" {
		return errors.New("specFile URL must include a host")
	}
	if parsed.User != nil {
		return errors.New("specFile URL must not contain user information")
	}
	if parsed.Fragment != "" {
		return errors.New("specFile URL must not contain a fragment")
	}
	return nil
}

func isRemoteSpecification(location string) bool {
	parsed, err := url.Parse(strings.TrimSpace(location))
	return err == nil && (strings.EqualFold(parsed.Scheme, "http") || strings.EqualFold(parsed.Scheme, "https"))
}

func validHTTPToken(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			strings.ContainsRune("!#$%&'*+-.^_`|~", character) {
			continue
		}
		return false
	}
	return true
}

func (authentication *fileAuthenticationConfig) toConfig() *AuthenticationConfig {
	if authentication == nil {
		return nil
	}
	return &AuthenticationConfig{
		SecurityScheme: authentication.SecurityScheme,
		Credential:     authentication.Credential,
		Username:       authentication.Username,
		Password:       authentication.Password,
	}
}

func cloneAuthentication(authentication *AuthenticationConfig) *AuthenticationConfig {
	if authentication == nil {
		return nil
	}
	copyAuthentication := *authentication
	return &copyAuthentication
}

func parseRequestTimeout(raw string) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse requestTimeout: %w", err)
	}
	return duration, nil
}

func validateBaseURL(raw string) error {
	if raw == "" {
		return errors.New("baseUrl is required")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("parse baseUrl: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" {
		return errors.New("baseUrl must be an absolute HTTP or HTTPS URL")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("baseUrl must not contain a query or fragment")
	}
	if parsed.User != nil {
		return errors.New("baseUrl must not contain user information")
	}
	return nil
}

func normalizeEntity(entity EntityConfig) (EntityConfig, error) {
	normalized := EntityConfig{
		Name:            strings.TrimSpace(entity.Name),
		ListOperation:   strings.TrimSpace(entity.ListOperation),
		ResponsePath:    strings.TrimSpace(entity.ResponsePath),
		PrimaryKey:      make([]string, 0, len(entity.PrimaryKey)),
		QueryParameters: make([]QueryParameterConfig, 0, len(entity.QueryParameters)),
		EqualityFilters: make([]EqualityFilterConfig, 0, len(entity.EqualityFilters)),
		Mutations:       &EntityMutationConfig{},
	}
	if normalized.Name == "" {
		return EntityConfig{}, errors.New("name is required")
	}
	if normalized.ListOperation == "" {
		return EntityConfig{}, errors.New("listOperation is required")
	}
	if normalized.ResponsePath != "" && !strings.HasPrefix(normalized.ResponsePath, "/") {
		return EntityConfig{}, errors.New("responsePath must be empty or an RFC 6901 JSON Pointer")
	}
	if _, err := parseJSONPointer(normalized.ResponsePath); err != nil {
		return EntityConfig{}, fmt.Errorf("responsePath: %w", err)
	}

	keys := make(map[string]struct{}, len(entity.PrimaryKey))
	for index, rawKey := range entity.PrimaryKey {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			return EntityConfig{}, fmt.Errorf("primaryKey %d must not be blank", index)
		}
		if _, exists := keys[key]; exists {
			return EntityConfig{}, fmt.Errorf("duplicate primaryKey field %q", key)
		}
		keys[key] = struct{}{}
		normalized.PrimaryKey = append(normalized.PrimaryKey, key)
	}
	queryParameters := make(map[string]struct{}, len(entity.QueryParameters))
	for index, candidate := range entity.QueryParameters {
		parameter := QueryParameterConfig{
			Name:  strings.TrimSpace(candidate.Name),
			Value: candidate.Value,
		}
		if parameter.Name == "" {
			return EntityConfig{}, fmt.Errorf("queryParameters %d name is required", index)
		}
		if strings.ContainsAny(parameter.Name, "\r\n") {
			return EntityConfig{}, fmt.Errorf("query parameter %q contains a newline", parameter.Name)
		}
		if _, exists := queryParameters[parameter.Name]; exists {
			return EntityConfig{}, fmt.Errorf("duplicate query parameter %q", parameter.Name)
		}
		queryParameters[parameter.Name] = struct{}{}
		normalized.QueryParameters = append(normalized.QueryParameters, parameter)
	}
	equalityFields := make(map[string]struct{}, len(entity.EqualityFilters))
	equalityParameters := make(map[string]struct{}, len(entity.EqualityFilters))
	for index, candidate := range entity.EqualityFilters {
		filter := EqualityFilterConfig{
			Field:     strings.TrimSpace(candidate.Field),
			Parameter: strings.TrimSpace(candidate.Parameter),
		}
		if filter.Field == "" {
			return EntityConfig{}, fmt.Errorf("equalityFilters %d field is required", index)
		}
		if filter.Parameter == "" {
			return EntityConfig{}, fmt.Errorf("equalityFilters %d parameter is required", index)
		}
		lookupField := strings.ToUpper(filter.Field)
		if _, exists := equalityFields[lookupField]; exists {
			return EntityConfig{}, fmt.Errorf("duplicate equality filter field %q", filter.Field)
		}
		if _, exists := equalityParameters[filter.Parameter]; exists {
			return EntityConfig{}, fmt.Errorf("duplicate equality filter parameter %q", filter.Parameter)
		}
		if _, exists := queryParameters[filter.Parameter]; exists {
			return EntityConfig{}, fmt.Errorf("query parameter %q cannot be both static and filter-bound", filter.Parameter)
		}
		equalityFields[lookupField] = struct{}{}
		equalityParameters[filter.Parameter] = struct{}{}
		normalized.EqualityFilters = append(normalized.EqualityFilters, filter)
	}
	pagination, err := normalizePagination(entity.Pagination)
	if err != nil {
		return EntityConfig{}, fmt.Errorf("pagination: %w", err)
	}
	normalized.Pagination = pagination
	if pagination != nil {
		for _, parameter := range paginationParameterNames(pagination) {
			if _, exists := queryParameters[parameter]; exists {
				return EntityConfig{}, fmt.Errorf("query parameter %q conflicts with pagination", parameter)
			}
			if _, exists := equalityParameters[parameter]; exists {
				return EntityConfig{}, fmt.Errorf("equality filter parameter %q conflicts with pagination", parameter)
			}
		}
	}
	mutations, err := normalizeMutations(entity.Mutations)
	if err != nil {
		return EntityConfig{}, fmt.Errorf("mutations: %w", err)
	}
	normalized.Mutations = mutations

	return normalized, nil
}

func normalizeMutations(mutations *EntityMutationConfig) (*EntityMutationConfig, error) {
	if mutations == nil {
		return nil, nil
	}
	normalized := &EntityMutationConfig{}
	operations := []struct {
		name   string
		source *MutationOperationConfig
		target **MutationOperationConfig
	}{
		{name: "insert", source: mutations.Insert, target: &normalized.Insert},
		{name: "update", source: mutations.Update, target: &normalized.Update},
		{name: "delete", source: mutations.Delete, target: &normalized.Delete},
	}
	for _, candidate := range operations {
		operation, err := normalizeMutationOperation(candidate.source)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", candidate.name, err)
		}
		*candidate.target = operation
	}
	if normalized.Insert == nil && normalized.Update == nil && normalized.Delete == nil {
		return nil, errors.New("at least one mutation operation is required")
	}
	return normalized, nil
}

func normalizeMutationOperation(operation *MutationOperationConfig) (*MutationOperationConfig, error) {
	if operation == nil {
		return nil, nil
	}
	normalized := &MutationOperationConfig{
		Operation:       strings.TrimSpace(operation.Operation),
		PathParameters:  make([]PathParameterConfig, 0, len(operation.PathParameters)),
		QueryParameters: make([]QueryParameterConfig, 0, len(operation.QueryParameters)),
		BodyPath:        strings.TrimSpace(operation.BodyPath),
	}
	if normalized.Operation == "" {
		return nil, errors.New("operation is required")
	}
	if normalized.BodyPath != "" {
		if _, err := parseJSONPointer(normalized.BodyPath); err != nil {
			return nil, fmt.Errorf("bodyPath: %w", err)
		}
	}
	pathParameters := make(map[string]struct{}, len(operation.PathParameters))
	for index, candidate := range operation.PathParameters {
		parameter := PathParameterConfig{
			Parameter: strings.TrimSpace(candidate.Parameter),
			Field:     strings.TrimSpace(candidate.Field),
			Value:     candidate.Value,
		}
		if parameter.Parameter == "" {
			return nil, fmt.Errorf("pathParameters %d parameter is required", index)
		}
		if (parameter.Field == "") == (parameter.Value == "") {
			return nil, fmt.Errorf("path parameter %q must define exactly one of field or value", parameter.Parameter)
		}
		if _, exists := pathParameters[parameter.Parameter]; exists {
			return nil, fmt.Errorf("duplicate path parameter %q", parameter.Parameter)
		}
		pathParameters[parameter.Parameter] = struct{}{}
		normalized.PathParameters = append(normalized.PathParameters, parameter)
	}
	queryParameters := make(map[string]struct{}, len(operation.QueryParameters))
	for index, candidate := range operation.QueryParameters {
		parameter := QueryParameterConfig{Name: strings.TrimSpace(candidate.Name), Value: candidate.Value}
		if parameter.Name == "" {
			return nil, fmt.Errorf("queryParameters %d name is required", index)
		}
		if _, exists := queryParameters[parameter.Name]; exists {
			return nil, fmt.Errorf("duplicate query parameter %q", parameter.Name)
		}
		queryParameters[parameter.Name] = struct{}{}
		normalized.QueryParameters = append(normalized.QueryParameters, parameter)
	}
	return normalized, nil
}

func (entity fileEntityConfig) queryParameters() []QueryParameterConfig {
	parameters := make([]QueryParameterConfig, len(entity.QueryParameters))
	for index, parameter := range entity.QueryParameters {
		parameters[index] = QueryParameterConfig{Name: parameter.Name, Value: parameter.Value}
	}
	return parameters
}

func (entity fileEntityConfig) equalityFilters() []EqualityFilterConfig {
	filters := make([]EqualityFilterConfig, len(entity.EqualityFilters))
	for index, filter := range entity.EqualityFilters {
		filters[index] = EqualityFilterConfig{Field: filter.Field, Parameter: filter.Parameter}
	}
	return filters
}

func (mutations *fileEntityMutationConfig) toConfig() *EntityMutationConfig {
	if mutations == nil {
		return nil
	}
	return &EntityMutationConfig{
		Insert: mutations.Insert.toConfig(),
		Update: mutations.Update.toConfig(),
		Delete: mutations.Delete.toConfig(),
	}
}

func (operation *fileMutationOperationConfig) toConfig() *MutationOperationConfig {
	if operation == nil {
		return nil
	}
	pathParameters := make([]PathParameterConfig, len(operation.PathParameters))
	for index, parameter := range operation.PathParameters {
		pathParameters[index] = PathParameterConfig{
			Parameter: parameter.Parameter,
			Field:     parameter.Field,
			Value:     parameter.Value,
		}
	}
	queryParameters := make([]QueryParameterConfig, len(operation.QueryParameters))
	for index, parameter := range operation.QueryParameters {
		queryParameters[index] = QueryParameterConfig{Name: parameter.Name, Value: parameter.Value}
	}
	return &MutationOperationConfig{
		Operation:       operation.Operation,
		PathParameters:  pathParameters,
		QueryParameters: queryParameters,
		BodyPath:        operation.BodyPath,
	}
}

func paginationParameterNames(pagination *PaginationConfig) []string {
	parameters := make([]string, 0, 2)
	if pagination.PageSizeParameter != "" {
		parameters = append(parameters, pagination.PageSizeParameter)
	}
	switch pagination.Mode {
	case PaginationModeOffset:
		parameters = append(parameters, pagination.OffsetParameter)
	case PaginationModePage:
		parameters = append(parameters, pagination.PageParameter)
	case PaginationModeCursor:
		parameters = append(parameters, pagination.CursorParameter)
	}
	return parameters
}

func (pagination *filePaginationConfig) toConfig() *PaginationConfig {
	if pagination == nil {
		return nil
	}
	return &PaginationConfig{
		Mode:              PaginationMode(pagination.Mode),
		PageSize:          pagination.PageSize,
		PageSizeParameter: pagination.PageSizeParameter,
		OffsetParameter:   pagination.OffsetParameter,
		PageParameter:     pagination.PageParameter,
		StartPage:         cloneUint64(pagination.StartPage),
		CursorParameter:   pagination.CursorParameter,
		NextCursorPath:    pagination.NextCursorPath,
		HasMorePath:       pagination.HasMorePath,
		MaxPages:          pagination.MaxPages,
	}
}

func normalizePagination(pagination *PaginationConfig) (*PaginationConfig, error) {
	if pagination == nil {
		return nil, nil
	}
	normalized := &PaginationConfig{
		Mode:              PaginationMode(strings.ToUpper(strings.TrimSpace(string(pagination.Mode)))),
		PageSize:          pagination.PageSize,
		PageSizeParameter: strings.TrimSpace(pagination.PageSizeParameter),
		OffsetParameter:   strings.TrimSpace(pagination.OffsetParameter),
		PageParameter:     strings.TrimSpace(pagination.PageParameter),
		StartPage:         cloneUint64(pagination.StartPage),
		CursorParameter:   strings.TrimSpace(pagination.CursorParameter),
		NextCursorPath:    strings.TrimSpace(pagination.NextCursorPath),
		HasMorePath:       strings.TrimSpace(pagination.HasMorePath),
		MaxPages:          pagination.MaxPages,
	}
	if normalized.PageSize == 0 {
		return nil, errors.New("pageSize must be greater than zero")
	}
	if normalized.MaxPages == 0 {
		normalized.MaxPages = 10000
	}
	parameters := []struct {
		name  string
		value string
	}{
		{name: "pageSizeParameter", value: normalized.PageSizeParameter},
		{name: "offsetParameter", value: normalized.OffsetParameter},
		{name: "pageParameter", value: normalized.PageParameter},
		{name: "cursorParameter", value: normalized.CursorParameter},
	}
	for _, parameter := range parameters {
		if strings.ContainsAny(parameter.value, "\r\n") {
			return nil, fmt.Errorf("%s contains a newline", parameter.name)
		}
	}

	switch normalized.Mode {
	case PaginationModeOffset:
		if normalized.OffsetParameter == "" {
			return nil, errors.New("offsetParameter is required for OFFSET pagination")
		}
	case PaginationModePage:
		if normalized.PageParameter == "" {
			return nil, errors.New("pageParameter is required for PAGE pagination")
		}
		if normalized.StartPage == nil {
			startPage := uint64(1)
			normalized.StartPage = &startPage
		}
	case PaginationModeCursor:
		if normalized.CursorParameter == "" {
			return nil, errors.New("cursorParameter is required for CURSOR pagination")
		}
		if normalized.NextCursorPath == "" {
			return nil, errors.New("nextCursorPath is required for CURSOR pagination")
		}
		if _, err := parseJSONPointer(normalized.NextCursorPath); err != nil {
			return nil, fmt.Errorf("nextCursorPath: %w", err)
		}
		if normalized.HasMorePath != "" {
			if _, err := parseJSONPointer(normalized.HasMorePath); err != nil {
				return nil, fmt.Errorf("hasMorePath: %w", err)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported mode %q", pagination.Mode)
	}
	activeParameter := normalized.OffsetParameter
	if normalized.Mode == PaginationModePage {
		activeParameter = normalized.PageParameter
	}
	if normalized.Mode == PaginationModeCursor {
		activeParameter = normalized.CursorParameter
	}
	if normalized.PageSizeParameter != "" && normalized.PageSizeParameter == activeParameter {
		return nil, errors.New("pageSizeParameter must differ from the pagination parameter")
	}
	return normalized, nil
}

func cloneUint64(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}
