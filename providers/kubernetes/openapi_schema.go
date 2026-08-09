package kubernetes

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/openapi"
)

const openAPIJSONContentType = "application/json"

type openAPIGVK struct {
	Group   string `json:"group"`
	Version string `json:"version"`
	Kind    string `json:"kind"`
}

type openAPISchema struct {
	Ref                  string                    `json:"$ref"`
	Type                 string                    `json:"type"`
	Format               string                    `json:"format"`
	Description          string                    `json:"description"`
	Nullable             bool                      `json:"nullable"`
	ReadOnly             bool                      `json:"readOnly"`
	Properties           map[string]*openAPISchema `json:"properties"`
	Items                *openAPISchema            `json:"items"`
	AdditionalProperties json.RawMessage           `json:"additionalProperties"`
	Required             []string                  `json:"required"`
	AllOf                []*openAPISchema          `json:"allOf"`
	OneOf                []*openAPISchema          `json:"oneOf"`
	AnyOf                []*openAPISchema          `json:"anyOf"`
	GroupVersionKinds    []openAPIGVK              `json:"x-kubernetes-group-version-kind"`
}

type openAPIDocument struct {
	Components struct {
		Schemas map[string]*openAPISchema `json:"schemas"`
	} `json:"components"`
}

type openAPIDocumentCacheEntry struct {
	serverRelativeURL string
	document          *openAPIDocument
}

type openAPISchemaCache struct {
	mu        sync.RWMutex
	documents map[string]openAPIDocumentCacheEntry
}

func (c *openAPISchemaCache) get(pathKey string, serverRelativeURL string) *openAPIDocument {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	entry, ok := c.documents[pathKey]
	c.mu.RUnlock()
	if !ok || entry.serverRelativeURL != serverRelativeURL {
		return nil
	}
	return entry.document
}

func (c *openAPISchemaCache) put(pathKey string, serverRelativeURL string, document *openAPIDocument) {
	if c == nil || document == nil {
		return
	}
	c.mu.Lock()
	if c.documents == nil {
		c.documents = make(map[string]openAPIDocumentCacheEntry)
	}
	c.documents[pathKey] = openAPIDocumentCacheEntry{
		serverRelativeURL: serverRelativeURL,
		document:          document,
	}
	c.mu.Unlock()
}

type openAPISchemaResolver struct {
	ctx   context.Context
	paths map[string]openapi.GroupVersion
	cache *openAPISchemaCache
}

type resolvedOpenAPISchema struct {
	document *openAPIDocument
	schema   *openAPISchema
}

func newOpenAPISchemaResolver(
	ctx context.Context,
	discoveryClient discovery.DiscoveryInterface,
	cache *openAPISchemaCache,
) *openAPISchemaResolver {
	if discoveryClient == nil || ctx.Err() != nil {
		return nil
	}

	var client openapi.Client
	func() {
		defer func() {
			_ = recover()
		}()
		client = discoveryClient.OpenAPIV3()
	}()
	if client == nil {
		return nil
	}

	paths, err := client.Paths()
	if err != nil || len(paths) == 0 || ctx.Err() != nil {
		return nil
	}

	return &openAPISchemaResolver{
		ctx:   ctx,
		paths: paths,
		cache: cache,
	}
}

func schemaExpansionConfigured(config SchemaConfig) bool {
	if config.FieldExpansionDepth > 0 {
		return true
	}
	for _, resource := range config.Resources {
		if resource.FieldExpansionDepth != nil && *resource.FieldExpansionDepth > 0 {
			return true
		}
	}
	return false
}

func openAPIPathKey(groupVersion schema.GroupVersion) string {
	if groupVersion.Group == "" {
		return "api/" + groupVersion.Version
	}
	return "apis/" + groupVersion.Group + "/" + groupVersion.Version
}

func (r *openAPISchemaResolver) schemaFor(
	groupVersion schema.GroupVersion,
	kind string,
) (*resolvedOpenAPISchema, bool) {
	if r == nil || r.ctx.Err() != nil {
		return nil, false
	}

	pathKey := openAPIPathKey(groupVersion)
	groupVersionSchema := r.paths[pathKey]
	if groupVersionSchema == nil {
		return nil, false
	}

	serverRelativeURL := groupVersionSchema.ServerRelativeURL()
	document := r.cache.get(pathKey, serverRelativeURL)
	if document == nil {
		contents, err := groupVersionSchema.Schema(openAPIJSONContentType)
		if err != nil || r.ctx.Err() != nil {
			return nil, false
		}
		parsed := &openAPIDocument{}
		if err := json.Unmarshal(contents, parsed); err != nil {
			return nil, false
		}
		document = parsed
		r.cache.put(pathKey, serverRelativeURL, document)
	}

	for _, candidate := range document.Components.Schemas {
		if candidate == nil {
			continue
		}
		for _, gvk := range candidate.GroupVersionKinds {
			if gvk.Group == groupVersion.Group &&
				gvk.Version == groupVersion.Version &&
				gvk.Kind == kind {
				return &resolvedOpenAPISchema{
					document: document,
					schema:   candidate,
				}, true
			}
		}
	}

	return nil, false
}

func (d *openAPIDocument) resolve(candidate *openAPISchema) *openAPISchema {
	return d.resolveSeen(candidate, make(map[string]struct{}))
}

func (d *openAPIDocument) resolveSeen(
	candidate *openAPISchema,
	seen map[string]struct{},
) *openAPISchema {
	if candidate == nil {
		return nil
	}
	if candidate.Ref != "" {
		const prefix = "#/components/schemas/"
		if !strings.HasPrefix(candidate.Ref, prefix) {
			return candidate
		}
		name := strings.TrimPrefix(candidate.Ref, prefix)
		if _, exists := seen[name]; exists {
			return candidate
		}
		target := d.Components.Schemas[name]
		if target == nil {
			return candidate
		}
		seen[name] = struct{}{}
		return d.resolveSeen(target, seen)
	}

	if candidate.Type == "" &&
		len(candidate.Properties) == 0 &&
		len(candidate.AllOf) == 1 &&
		len(candidate.OneOf) == 0 &&
		len(candidate.AnyOf) == 0 {
		return d.resolveSeen(candidate.AllOf[0], seen)
	}

	return candidate
}

func resourceTableMetadataWithSchema(
	descriptor *resourceDescriptor,
	resolver *openAPISchemaResolver,
	depth int,
) *providerv1.TableMetadata {
	table := resourceTableMetadata(descriptor)
	baseColumnCount := len(table.GetColumns())
	if depth <= 0 || resolver == nil {
		return table
	}

	resolved, ok := resolver.schemaFor(descriptor.groupVersion, descriptor.resource.Kind)
	if !ok {
		return table
	}

	existing := make(map[string]struct{}, len(table.GetColumns()))
	for _, column := range table.GetColumns() {
		existing[strings.ToUpper(column.GetName())] = struct{}{}
	}

	root := resolved.document.resolve(resolved.schema)
	if root == nil {
		return table
	}
	writeColumns := hasVerb(descriptor.resource.Verbs, "create") ||
		hasVerb(descriptor.resource.Verbs, "update") ||
		hasVerb(descriptor.resource.Verbs, "patch")

	for _, rootName := range []string{"metadata", "spec", "status"} {
		property := root.Properties[rootName]
		if property == nil {
			continue
		}
		appendExpandedOpenAPIColumns(
			table,
			resolved.document,
			property,
			[]string{rootName},
			depth,
			existing,
			writeColumns,
			false,
		)
	}

	if table.Properties == nil {
		table.Properties = make(map[string]string)
	}
	if len(table.GetColumns()) > baseColumnCount {
		table.Properties["kubernetes.schema_expansion"] = "openapi_v3"
		table.Properties["kubernetes.field_expansion_depth"] = strconv.Itoa(depth)
	}

	return table
}

func appendExpandedOpenAPIColumns(
	table *providerv1.TableMetadata,
	document *openAPIDocument,
	candidate *openAPISchema,
	path []string,
	maxDepth int,
	existing map[string]struct{},
	writeColumns bool,
	parentReadOnly bool,
) {
	if candidate == nil || len(path) == 0 || len(path) > maxDepth {
		return
	}

	resolved := document.resolve(candidate)
	if resolved == nil {
		return
	}
	readOnly := parentReadOnly || candidate.ReadOnly || resolved.ReadOnly

	if openAPISchemaIsScalar(resolved) {
		appendOpenAPIColumn(table, resolved, path, existing, openAPISchemaValueType(resolved), writeColumns, readOnly)
		return
	}

	if openAPISchemaIsCollection(resolved) ||
		len(resolved.OneOf) > 0 ||
		len(resolved.AnyOf) > 0 {
		appendOpenAPIColumn(
			table,
			resolved,
			path,
			existing,
			kublingv1.ValueType_VALUE_TYPE_JSON,
			writeColumns,
			readOnly,
		)
		return
	}

	if len(path) == maxDepth || len(resolved.Properties) == 0 {
		appendOpenAPIColumn(
			table,
			resolved,
			path,
			existing,
			kublingv1.ValueType_VALUE_TYPE_JSON,
			writeColumns,
			readOnly,
		)
		return
	}

	names := make([]string, 0, len(resolved.Properties))
	for name := range resolved.Properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		appendExpandedOpenAPIColumns(
			table,
			document,
			resolved.Properties[name],
			appendPath(path, name),
			maxDepth,
			existing,
			writeColumns,
			readOnly,
		)
	}
}

func appendOpenAPIColumn(
	table *providerv1.TableMetadata,
	schema *openAPISchema,
	path []string,
	existing map[string]struct{},
	valueType kublingv1.ValueType,
	writeColumns bool,
	readOnly bool,
) {
	name := strings.Join(path, "__")
	key := strings.ToUpper(name)
	if _, exists := existing[key]; exists {
		return
	}

	nullable := true
	updatable := writeColumns && !readOnly && kubernetesFieldPathWritable(path)
	column := &providerv1.ColumnMetadata{
		Name:          name,
		SourceName:    strings.Join(path, "."),
		Type:          valueType,
		NativeType:    openAPINativeType(schema),
		Nullable:      &nullable,
		Updatable:     &updatable,
		Searchability: providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
		Annotation:    strings.TrimSpace(schema.Description),
	}

	table.Columns = append(table.Columns, column)
	existing[key] = struct{}{}
}

func appendPath(path []string, value string) []string {
	result := make([]string, 0, len(path)+1)
	result = append(result, path...)
	result = append(result, value)
	return result
}

func openAPISchemaIsScalar(schema *openAPISchema) bool {
	switch strings.ToLower(schema.Type) {
	case "string", "boolean", "integer", "number":
		return true
	default:
		return false
	}
}

func openAPISchemaIsCollection(schema *openAPISchema) bool {
	if strings.EqualFold(schema.Type, "array") || schema.Items != nil {
		return true
	}
	if len(schema.AdditionalProperties) > 0 {
		raw := strings.TrimSpace(string(schema.AdditionalProperties))
		return raw != "" && raw != "false" && raw != "null"
	}
	return false
}

func openAPISchemaValueType(schema *openAPISchema) kublingv1.ValueType {
	switch strings.ToLower(schema.Type) {
	case "boolean":
		return kublingv1.ValueType_VALUE_TYPE_BOOLEAN
	case "integer":
		if strings.EqualFold(schema.Format, "int64") {
			return kublingv1.ValueType_VALUE_TYPE_LONG
		}
		return kublingv1.ValueType_VALUE_TYPE_INTEGER
	case "number":
		if strings.EqualFold(schema.Format, "float") {
			return kublingv1.ValueType_VALUE_TYPE_FLOAT
		}
		return kublingv1.ValueType_VALUE_TYPE_DOUBLE
	case "string":
		return kublingv1.ValueType_VALUE_TYPE_STRING
	default:
		return kublingv1.ValueType_VALUE_TYPE_JSON
	}
}

func openAPINativeType(schema *openAPISchema) string {
	if schema == nil {
		return "object"
	}
	value := strings.ToLower(strings.TrimSpace(schema.Type))
	if value == "" {
		value = "object"
	}
	if strings.TrimSpace(schema.Format) != "" {
		return value + "/" + strings.TrimSpace(schema.Format)
	}
	if openAPISchemaIsCollection(schema) && value == "object" {
		return "object"
	}
	return value
}
