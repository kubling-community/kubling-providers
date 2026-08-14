package openapi

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"github.com/pb33f/libopenapi/datamodel/high/base"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
)

type catalog struct {
	metadata *providerv1.SchemaMetadata
	entities map[string]*entityDescriptor
}

type entityDescriptor struct {
	config           EntityConfig
	path             string
	operation        *v3.Operation
	queryParameters  map[string]*v3.Parameter
	equalityFilters  map[string]string
	requiredBindings map[string]string
	rowSchema        *base.Schema
	table            *providerv1.TableMetadata
	insert           *mutationDescriptor
	update           *mutationDescriptor
	delete           *mutationDescriptor
}

type mutationDescriptor struct {
	config          *MutationOperationConfig
	method          string
	path            string
	operation       *v3.Operation
	pathParameters  map[string]*v3.Parameter
	queryParameters map[string]*v3.Parameter
	bodySchema      *base.Schema
	bodyProperties  map[string]*base.SchemaProxy
	bodyRequired    map[string]bool
}

func buildCatalog(config Config, document *v3.Document) (*catalog, error) {
	if document == nil {
		return nil, errors.New("OpenAPI document is required")
	}

	discovered, err := discoverEntities(document, config.Discovery, config.Entities)
	if err != nil {
		return nil, err
	}
	entities := append(append([]EntityConfig(nil), config.Entities...), discovered...)
	if len(entities) == 0 {
		return nil, errors.New("OpenAPI discovery found no eligible entities")
	}

	result := &catalog{
		metadata: &providerv1.SchemaMetadata{
			Properties: documentProperties(document),
		},
		entities: make(map[string]*entityDescriptor, len(entities)),
	}
	if config.Namespace != "" {
		result.metadata.Namespaces = []*providerv1.NamespaceMetadata{{
			Name:       config.Namespace,
			Annotation: documentTitle(document),
		}}
	}

	for _, entity := range entities {
		descriptor, err := describeEntity(document, entity)
		if err != nil {
			return nil, fmt.Errorf("entity %q: %w", entity.Name, err)
		}
		table, err := tableMetadata(config.Namespace, descriptor)
		if err != nil {
			return nil, fmt.Errorf("entity %q: %w", entity.Name, err)
		}
		lookupName := strings.ToUpper(entity.Name)
		if existing := result.entities[lookupName]; existing != nil {
			return nil, fmt.Errorf(
				"entity name %q is produced by both operations %q and %q; add an explicit entity mapping",
				entity.Name,
				existing.config.ListOperation,
				entity.ListOperation,
			)
		}
		result.metadata.Tables = append(result.metadata.Tables, table)
		descriptor.table = table
		result.entities[lookupName] = descriptor
	}

	sort.Slice(result.metadata.Tables, func(left, right int) bool {
		return result.metadata.Tables[left].GetName() < result.metadata.Tables[right].GetName()
	})
	return result, nil
}

func documentProperties(document *v3.Document) map[string]string {
	properties := map[string]string{
		"openapi.version": document.Version,
	}
	if document.Info != nil {
		if document.Info.Title != "" {
			properties["openapi.title"] = document.Info.Title
		}
		if document.Info.Version != "" {
			properties["openapi.api_version"] = document.Info.Version
		}
	}
	return properties
}

func documentTitle(document *v3.Document) string {
	if document.Info == nil {
		return ""
	}
	return document.Info.Title
}

func describeEntity(document *v3.Document, config EntityConfig) (*entityDescriptor, error) {
	path, pathItem, operation, err := findGETOperation(document, config.ListOperation)
	if err != nil {
		return nil, err
	}
	queryParameters, err := operationQueryParameters(pathItem, operation)
	if err != nil {
		return nil, err
	}
	responseSchema, err := successfulJSONResponseSchema(operation)
	if err != nil {
		return nil, err
	}
	targetSchema, err := schemaAtJSONPointer(responseSchema, config.ResponsePath)
	if err != nil {
		return nil, err
	}
	rowSchema, err := arrayItemSchema(targetSchema)
	if err != nil {
		return nil, fmt.Errorf("responsePath %q: %w", config.ResponsePath, err)
	}
	if _, _, err := objectShape(rowSchema, make(map[*base.Schema]bool)); err != nil {
		return nil, fmt.Errorf("response row schema: %w", err)
	}
	descriptor := &entityDescriptor{
		config:           config,
		path:             path,
		operation:        operation,
		queryParameters:  queryParameters,
		equalityFilters:  make(map[string]string, len(config.EqualityFilters)),
		requiredBindings: make(map[string]string),
		rowSchema:        rowSchema,
	}
	if err := descriptor.validateQueryBindings(); err != nil {
		return nil, err
	}
	if err := descriptor.describeMutations(document); err != nil {
		return nil, err
	}
	return descriptor, nil
}

func (descriptor *entityDescriptor) describeMutations(document *v3.Document) error {
	if descriptor.config.Mutations == nil {
		return nil
	}
	mutations := []struct {
		name    string
		config  *MutationOperationConfig
		methods []string
		target  **mutationDescriptor
		body    bool
	}{
		{name: "insert", config: descriptor.config.Mutations.Insert, methods: []string{httpMethodPost}, target: &descriptor.insert, body: true},
		{name: "update", config: descriptor.config.Mutations.Update, methods: []string{httpMethodPatch, httpMethodPut}, target: &descriptor.update, body: true},
		{name: "delete", config: descriptor.config.Mutations.Delete, methods: []string{httpMethodDelete}, target: &descriptor.delete},
	}
	for _, candidate := range mutations {
		if candidate.config == nil {
			continue
		}
		mutation, err := describeMutation(document, candidate.config, candidate.methods, candidate.body)
		if err != nil {
			return fmt.Errorf("%s mutation: %w", candidate.name, err)
		}
		*candidate.target = mutation
	}
	return nil
}

const (
	httpMethodDelete = "DELETE"
	httpMethodPatch  = "PATCH"
	httpMethodPost   = "POST"
	httpMethodPut    = "PUT"
)

func describeMutation(
	document *v3.Document,
	config *MutationOperationConfig,
	allowedMethods []string,
	requiresBody bool,
) (*mutationDescriptor, error) {
	method, path, pathItem, operation, err := findOperation(document, config.Operation)
	if err != nil {
		return nil, err
	}
	if !slices.Contains(allowedMethods, method) {
		return nil, fmt.Errorf("operationId %q uses %s, want %s", config.Operation, method, strings.Join(allowedMethods, " or "))
	}
	pathParameters, queryParameters, err := operationParameters(pathItem, operation)
	if err != nil {
		return nil, err
	}
	descriptor := &mutationDescriptor{
		config:          config,
		method:          method,
		path:            path,
		operation:       operation,
		pathParameters:  pathParameters,
		queryParameters: queryParameters,
	}
	if err := descriptor.validateBindings(); err != nil {
		return nil, err
	}
	if requiresBody {
		descriptor.bodySchema, err = jsonRequestBodySchema(operation, config.BodyPath)
		if err != nil {
			return nil, err
		}
		descriptor.bodyProperties, descriptor.bodyRequired, err = objectShape(descriptor.bodySchema, make(map[*base.Schema]bool))
		if err != nil {
			return nil, fmt.Errorf("request body schema: %w", err)
		}
	}
	return descriptor, nil
}

func findOperation(document *v3.Document, operationID string) (string, string, *v3.PathItem, *v3.Operation, error) {
	if document.Paths == nil || document.Paths.PathItems == nil {
		return "", "", nil, nil, errors.New("OpenAPI document defines no paths")
	}
	var matchedMethod string
	var matchedPath string
	var matchedPathItem *v3.PathItem
	var matchedOperation *v3.Operation
	for path, item := range document.Paths.PathItems.FromOldest() {
		if item == nil {
			continue
		}
		operations := []struct {
			method    string
			operation *v3.Operation
		}{
			{method: "GET", operation: item.Get},
			{method: httpMethodPut, operation: item.Put},
			{method: httpMethodPost, operation: item.Post},
			{method: httpMethodDelete, operation: item.Delete},
			{method: httpMethodPatch, operation: item.Patch},
		}
		for _, candidate := range operations {
			if candidate.operation == nil || candidate.operation.OperationId != operationID {
				continue
			}
			if matchedOperation != nil {
				return "", "", nil, nil, fmt.Errorf("operationId %q is not unique", operationID)
			}
			matchedMethod = candidate.method
			matchedPath = path
			matchedPathItem = item
			matchedOperation = candidate.operation
		}
	}
	if matchedOperation == nil {
		return "", "", nil, nil, fmt.Errorf("operationId %q was not found", operationID)
	}
	return matchedMethod, matchedPath, matchedPathItem, matchedOperation, nil
}

func operationParameters(pathItem *v3.PathItem, operation *v3.Operation) (map[string]*v3.Parameter, map[string]*v3.Parameter, error) {
	pathParameters := make(map[string]*v3.Parameter)
	queryParameters := make(map[string]*v3.Parameter)
	add := func(parameter *v3.Parameter) error {
		if parameter == nil {
			return errors.New("operation contains an empty parameter")
		}
		name := strings.TrimSpace(parameter.Name)
		location := strings.ToLower(strings.TrimSpace(parameter.In))
		if name == "" || location == "" {
			return errors.New("operation contains a parameter without name or location")
		}
		switch location {
		case "path":
			pathParameters[name] = parameter
		case "query":
			queryParameters[name] = parameter
		}
		return nil
	}
	if pathItem != nil {
		for _, parameter := range pathItem.Parameters {
			if err := add(parameter); err != nil {
				return nil, nil, err
			}
		}
	}
	if operation != nil {
		for _, parameter := range operation.Parameters {
			if err := add(parameter); err != nil {
				return nil, nil, err
			}
		}
	}
	return pathParameters, queryParameters, nil
}

func (descriptor *mutationDescriptor) validateBindings() error {
	configured := make(map[string]struct{}, len(descriptor.config.PathParameters))
	configuredQuery := make(map[string]struct{}, len(descriptor.config.QueryParameters))
	dynamicBindings := 0
	for _, binding := range descriptor.config.PathParameters {
		parameter := descriptor.pathParameters[binding.Parameter]
		if parameter == nil {
			return fmt.Errorf("path parameter %q is not defined by the operation", binding.Parameter)
		}
		if err := validateMutationPathParameter(parameter); err != nil {
			return fmt.Errorf("path parameter %q: %w", binding.Parameter, err)
		}
		if !strings.Contains(descriptor.path, "{"+binding.Parameter+"}") {
			return fmt.Errorf("path parameter %q is not present in operation path %q", binding.Parameter, descriptor.path)
		}
		if binding.Field != "" {
			dynamicBindings++
		}
		configured[binding.Parameter] = struct{}{}
	}
	for name := range descriptor.pathParameters {
		if _, exists := configured[name]; !exists {
			return fmt.Errorf("path parameter %q has no field or static binding", name)
		}
	}
	for _, parameter := range descriptor.config.QueryParameters {
		definition := descriptor.queryParameters[parameter.Name]
		if definition == nil {
			return fmt.Errorf("query parameter %q is not defined by the operation", parameter.Name)
		}
		if err := validateEqualityParameter(definition); err != nil {
			return fmt.Errorf("query parameter %q: %w", parameter.Name, err)
		}
		configuredQuery[parameter.Name] = struct{}{}
	}
	for name, parameter := range descriptor.queryParameters {
		if parameter.Required != nil && *parameter.Required {
			if _, exists := configuredQuery[name]; !exists {
				return fmt.Errorf("required query parameter %q has no static binding", name)
			}
		}
	}
	if descriptor.method != httpMethodPost && dynamicBindings == 0 {
		return errors.New("update and delete operations require at least one field-bound path parameter")
	}
	return nil
}

func validateMutationPathParameter(parameter *v3.Parameter) error {
	if parameter.Required == nil || !*parameter.Required {
		return errors.New("must be required")
	}
	if parameter.Style != "" && !strings.EqualFold(parameter.Style, "simple") {
		return fmt.Errorf("style %q is not supported", parameter.Style)
	}
	if parameter.Explode != nil && *parameter.Explode {
		return errors.New("explode=true is not supported")
	}
	return validateEqualityParameter(parameter)
}

func jsonRequestBodySchema(operation *v3.Operation, bodyPath string) (*base.Schema, error) {
	if operation.RequestBody == nil || operation.RequestBody.Content == nil {
		return nil, errors.New("operation defines no request body")
	}
	for mediaType, content := range operation.RequestBody.Content.FromOldest() {
		if !isJSONMediaType(mediaType) || content == nil || content.Schema == nil {
			continue
		}
		schema := content.Schema.Schema()
		if schema == nil {
			return nil, fmt.Errorf("build request body schema: %w", content.Schema.GetBuildError())
		}
		target, err := schemaAtJSONPointer(schema, bodyPath)
		if err != nil {
			return nil, fmt.Errorf("bodyPath %q: %w", bodyPath, err)
		}
		return target, nil
	}
	return nil, errors.New("operation has no JSON request body schema")
}

func findGETOperation(document *v3.Document, operationID string) (string, *v3.PathItem, *v3.Operation, error) {
	if document.Paths == nil || document.Paths.PathItems == nil {
		return "", nil, nil, errors.New("OpenAPI document defines no paths")
	}

	var matchedPath string
	var matchedPathItem *v3.PathItem
	var matchedOperation *v3.Operation
	for path, item := range document.Paths.PathItems.FromOldest() {
		if item == nil || item.Get == nil || item.Get.OperationId != operationID {
			continue
		}
		if matchedOperation != nil {
			return "", nil, nil, fmt.Errorf("GET operationId %q is not unique", operationID)
		}
		matchedPath = path
		matchedPathItem = item
		matchedOperation = item.Get
	}
	if matchedOperation == nil {
		return "", nil, nil, fmt.Errorf("GET operationId %q was not found", operationID)
	}
	return matchedPath, matchedPathItem, matchedOperation, nil
}

func operationQueryParameters(pathItem *v3.PathItem, operation *v3.Operation) (map[string]*v3.Parameter, error) {
	parameters := make(map[string]*v3.Parameter)
	add := func(parameter *v3.Parameter) error {
		if parameter == nil {
			return errors.New("list operation contains an empty parameter")
		}
		name := strings.TrimSpace(parameter.Name)
		location := strings.ToLower(strings.TrimSpace(parameter.In))
		if name == "" || location == "" {
			return errors.New("list operation contains a parameter without name or location")
		}
		if location == "query" {
			parameters[name] = parameter
		}
		return nil
	}
	if pathItem != nil {
		for _, parameter := range pathItem.Parameters {
			if err := add(parameter); err != nil {
				return nil, err
			}
		}
	}
	if operation != nil {
		for _, parameter := range operation.Parameters {
			if err := add(parameter); err != nil {
				return nil, err
			}
		}
	}
	return parameters, nil
}

func (descriptor *entityDescriptor) validateQueryBindings() error {
	bound := make(map[string]string)
	for _, configured := range descriptor.config.QueryParameters {
		if descriptor.queryParameters[configured.Name] == nil {
			return fmt.Errorf("query parameter %q is not defined by the list operation", configured.Name)
		}
		bound[configured.Name] = "static"
	}
	for _, configured := range descriptor.config.EqualityFilters {
		parameter := descriptor.queryParameters[configured.Parameter]
		if parameter == nil {
			return fmt.Errorf("equality filter parameter %q is not defined by the list operation", configured.Parameter)
		}
		if err := validateEqualityParameter(parameter); err != nil {
			return fmt.Errorf("equality filter parameter %q: %w", configured.Parameter, err)
		}
		descriptor.equalityFilters[strings.ToUpper(configured.Field)] = configured.Parameter
		bound[configured.Parameter] = configured.Field
	}
	if pagination := descriptor.config.Pagination; pagination != nil {
		for _, name := range paginationParameterNames(pagination) {
			if descriptor.queryParameters[name] == nil {
				return fmt.Errorf("pagination parameter %q is not defined by the list operation", name)
			}
			bound[name] = "pagination"
		}
	}
	for name, parameter := range descriptor.queryParameters {
		if parameter.Required != nil && *parameter.Required {
			if binding := bound[name]; binding == "" {
				return fmt.Errorf("required query parameter %q has no static, filter or pagination binding", name)
			} else if binding != "static" && binding != "pagination" {
				descriptor.requiredBindings[name] = binding
			}
		}
	}
	return nil
}

func validateEqualityParameter(parameter *v3.Parameter) error {
	if parameter.Schema == nil {
		return errors.New("must define a scalar schema")
	}
	schema := parameter.Schema.Schema()
	if schema == nil {
		return fmt.Errorf("build schema: %w", parameter.Schema.GetBuildError())
	}
	types := nonNullSchemaTypes(schema)
	if len(types) != 1 {
		return errors.New("must define exactly one non-null scalar type")
	}
	switch types[0] {
	case "boolean", "integer", "number", "string":
		return nil
	default:
		return fmt.Errorf("type %q is not scalar", types[0])
	}
}

func successfulJSONResponseSchema(operation *v3.Operation) (*base.Schema, error) {
	if operation.Responses == nil || operation.Responses.Codes == nil {
		return nil, errors.New("list operation defines no responses")
	}

	codes := make([]string, 0)
	for code := range operation.Responses.Codes.FromOldest() {
		if isSuccessfulResponseCode(code) {
			codes = append(codes, code)
		}
	}
	sort.Slice(codes, func(left, right int) bool {
		if codes[left] == "200" {
			return true
		}
		if codes[right] == "200" {
			return false
		}
		return codes[left] < codes[right]
	})

	for _, code := range codes {
		response := operation.Responses.Codes.GetOrZero(code)
		if response == nil || response.Content == nil {
			continue
		}
		mediaTypes := make([]string, 0)
		for mediaType := range response.Content.FromOldest() {
			if isJSONMediaType(mediaType) {
				mediaTypes = append(mediaTypes, mediaType)
			}
		}
		sort.Slice(mediaTypes, func(left, right int) bool {
			leftJSON := normalizedMediaType(mediaTypes[left]) == "application/json"
			rightJSON := normalizedMediaType(mediaTypes[right]) == "application/json"
			if leftJSON != rightJSON {
				return leftJSON
			}
			return mediaTypes[left] < mediaTypes[right]
		})
		for _, mediaType := range mediaTypes {
			content := response.Content.GetOrZero(mediaType)
			if content == nil || content.Schema == nil {
				continue
			}
			schema := content.Schema.Schema()
			if schema == nil {
				return nil, fmt.Errorf("build response schema for status %s: %w", code, content.Schema.GetBuildError())
			}
			return schema, nil
		}
	}
	return nil, errors.New("list operation has no successful JSON response schema")
}

func isSuccessfulResponseCode(code string) bool {
	if strings.EqualFold(code, "2XX") {
		return true
	}
	statusCode, err := strconv.Atoi(code)
	return err == nil && statusCode >= 200 && statusCode < 300
}

func isJSONMediaType(mediaType string) bool {
	normalized := normalizedMediaType(mediaType)
	return normalized == "application/json" || strings.HasSuffix(normalized, "+json")
}

func normalizedMediaType(mediaType string) string {
	mediaType, _, _ = strings.Cut(mediaType, ";")
	return strings.ToLower(strings.TrimSpace(mediaType))
}

func schemaAtJSONPointer(schema *base.Schema, pointer string) (*base.Schema, error) {
	tokens, err := parseJSONPointer(pointer)
	if err != nil {
		return nil, err
	}
	current := schema
	for _, token := range tokens {
		properties, _, err := objectShape(current, make(map[*base.Schema]bool))
		if err != nil {
			return nil, fmt.Errorf("resolve %q: %w", token, err)
		}
		property := properties[token]
		if property == nil {
			return nil, fmt.Errorf("responsePath property %q was not found", token)
		}
		current = property.Schema()
		if current == nil {
			return nil, fmt.Errorf("build responsePath property %q: %w", token, property.GetBuildError())
		}
	}
	return current, nil
}

func parseJSONPointer(pointer string) ([]string, error) {
	if pointer == "" {
		return nil, nil
	}
	if !strings.HasPrefix(pointer, "/") {
		return nil, errors.New("must start with /")
	}
	rawTokens := strings.Split(pointer[1:], "/")
	tokens := make([]string, len(rawTokens))
	for index, rawToken := range rawTokens {
		var token strings.Builder
		for position := 0; position < len(rawToken); position++ {
			if rawToken[position] != '~' {
				token.WriteByte(rawToken[position])
				continue
			}
			if position+1 >= len(rawToken) || (rawToken[position+1] != '0' && rawToken[position+1] != '1') {
				return nil, fmt.Errorf("invalid escape in token %q", rawToken)
			}
			position++
			if rawToken[position] == '0' {
				token.WriteByte('~')
			} else {
				token.WriteByte('/')
			}
		}
		tokens[index] = token.String()
	}
	return tokens, nil
}

func arrayItemSchema(schema *base.Schema) (*base.Schema, error) {
	if !hasSchemaType(schema, "array") || schema.Items == nil || !schema.Items.IsA() || schema.Items.A == nil {
		return nil, errors.New("schema must be an array with an item schema")
	}
	items := schema.Items.A.Schema()
	if items == nil {
		return nil, fmt.Errorf("build array item schema: %w", schema.Items.A.GetBuildError())
	}
	return items, nil
}

func objectShape(schema *base.Schema, visiting map[*base.Schema]bool) (map[string]*base.SchemaProxy, map[string]bool, error) {
	if schema == nil {
		return nil, nil, errors.New("schema is missing")
	}
	if visiting[schema] {
		return nil, nil, errors.New("circular object composition is not supported")
	}
	if len(schema.OneOf) > 0 || len(schema.AnyOf) > 0 {
		return nil, nil, errors.New("oneOf and anyOf row schemas are not supported")
	}
	if !hasSchemaType(schema, "object") && schema.Properties == nil && len(schema.AllOf) == 0 {
		return nil, nil, errors.New("schema must describe an object")
	}

	visiting[schema] = true
	defer delete(visiting, schema)
	properties := make(map[string]*base.SchemaProxy)
	required := make(map[string]bool)
	for _, component := range schema.AllOf {
		componentSchema := component.Schema()
		if componentSchema == nil {
			return nil, nil, fmt.Errorf("build allOf schema: %w", component.GetBuildError())
		}
		componentProperties, componentRequired, err := objectShape(componentSchema, visiting)
		if err != nil {
			return nil, nil, err
		}
		for name, property := range componentProperties {
			properties[name] = property
		}
		for name := range componentRequired {
			required[name] = true
		}
	}
	if schema.Properties != nil {
		for name, property := range schema.Properties.FromOldest() {
			properties[name] = property
		}
	}
	for _, name := range schema.Required {
		required[name] = true
	}
	if len(properties) == 0 {
		return nil, nil, errors.New("row object defines no properties")
	}
	return properties, required, nil
}

func tableMetadata(namespace string, descriptor *entityDescriptor) (*providerv1.TableMetadata, error) {
	properties, required, err := objectShape(descriptor.rowSchema, make(map[*base.Schema]bool))
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	sort.Strings(names)
	columns := make([]*providerv1.ColumnMetadata, 0, len(names))
	columnsByName := make(map[string]*providerv1.ColumnMetadata, len(names))
	columnsByLookupName := make(map[string]*providerv1.ColumnMetadata, len(names))
	for _, name := range names {
		column, err := columnMetadata(name, properties[name], required[name])
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", name, err)
		}
		columns = append(columns, column)
		columnsByName[name] = column
		columnsByLookupName[strings.ToUpper(name)] = column
	}
	for _, filter := range descriptor.config.EqualityFilters {
		column := columnsByLookupName[strings.ToUpper(filter.Field)]
		if column == nil {
			return nil, fmt.Errorf("equality filter field %q is not present in the response row schema", filter.Field)
		}
		column.Searchability = providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY
	}

	keys := make([]*providerv1.KeyMetadata, 0, 1)
	if len(descriptor.config.PrimaryKey) > 0 {
		for _, field := range descriptor.config.PrimaryKey {
			column := columnsByName[field]
			if column == nil {
				return nil, fmt.Errorf("primaryKey field %q is not present in the response row schema", field)
			}
			column.Nullable = new(false)
		}
		keys = append(keys, &providerv1.KeyMetadata{
			Name:    "PK_" + descriptor.config.Name,
			Kind:    providerv1.KeyKind_KEY_KIND_PRIMARY,
			Columns: append([]string(nil), descriptor.config.PrimaryKey...),
		})
	}
	if err := applyMutationMetadata(descriptor, columnsByLookupName); err != nil {
		return nil, err
	}

	updatable := descriptor.insert != nil || descriptor.update != nil || descriptor.delete != nil
	annotation := descriptor.operation.Summary
	if annotation == "" {
		annotation = descriptor.operation.Description
	}
	tableProperties := map[string]string{
		"openapi.method":        "GET",
		"openapi.operation_id":  descriptor.operation.OperationId,
		"openapi.path":          descriptor.path,
		"openapi.response_path": descriptor.config.ResponsePath,
	}
	for name, mutation := range map[string]*mutationDescriptor{
		"insert": descriptor.insert,
		"update": descriptor.update,
		"delete": descriptor.delete,
	} {
		if mutation != nil {
			tableProperties["openapi."+name+".method"] = mutation.method
			tableProperties["openapi."+name+".operation_id"] = mutation.operation.OperationId
			tableProperties["openapi."+name+".path"] = mutation.path
		}
	}
	table := &providerv1.TableMetadata{
		Name:       descriptor.config.Name,
		SourceName: descriptor.operation.OperationId,
		Kind:       providerv1.TableKind_TABLE_KIND_TABLE,
		Columns:    columns,
		Keys:       keys,
		Updatable:  &updatable,
		Annotation: annotation,
		Namespace:  namespace,
		Properties: tableProperties,
	}
	if stableKey := descriptor.config.StableKey; stableKey != nil {
		if err := providersdk.AddStablePrimaryKey(table, stableKey.Name, stableKey.Columns...); err != nil {
			return nil, fmt.Errorf("stableKey: %w", err)
		}
	}
	return table, nil
}

func applyMutationMetadata(
	descriptor *entityDescriptor,
	columns map[string]*providerv1.ColumnMetadata,
) error {
	writable := make(map[string]bool)
	insertRequired := make(map[string]bool)
	for _, mutation := range []*mutationDescriptor{descriptor.insert, descriptor.update} {
		if mutation == nil {
			continue
		}
		for sourceName, proxy := range mutation.bodyProperties {
			column := columns[strings.ToUpper(sourceName)]
			if column == nil || schemaProxyReadOnly(proxy) {
				continue
			}
			writable[strings.ToUpper(column.GetName())] = true
			if mutation == descriptor.insert && mutation.bodyRequired[sourceName] {
				insertRequired[strings.ToUpper(column.GetName())] = true
			}
		}
	}
	for _, mutation := range []*mutationDescriptor{descriptor.insert, descriptor.update, descriptor.delete} {
		if mutation == nil {
			continue
		}
		for _, binding := range mutation.config.PathParameters {
			if binding.Field == "" {
				continue
			}
			column := columns[strings.ToUpper(binding.Field)]
			if column == nil {
				return fmt.Errorf("%s mutation path field %q is not present in the response row schema", strings.ToLower(mutation.method), binding.Field)
			}
			if mutation == descriptor.insert {
				writable[strings.ToUpper(column.GetName())] = true
				insertRequired[strings.ToUpper(column.GetName())] = true
			}
		}
	}
	for _, column := range columns {
		lookup := strings.ToUpper(column.GetName())
		column.Updatable = new(writable[lookup])
		if descriptor.insert != nil {
			column.Nullable = new(!insertRequired[lookup])
		}
	}
	return nil
}

func schemaProxyReadOnly(proxy *base.SchemaProxy) bool {
	if proxy == nil {
		return false
	}
	schema := proxy.Schema()
	return schema != nil && schema.ReadOnly != nil && *schema.ReadOnly
}

func columnMetadata(name string, proxy *base.SchemaProxy, required bool) (*providerv1.ColumnMetadata, error) {
	if proxy == nil {
		return nil, errors.New("schema is missing")
	}
	schema := proxy.Schema()
	if schema == nil {
		return nil, fmt.Errorf("build schema: %w", proxy.GetBuildError())
	}
	valueType, nativeType := schemaValueType(schema)
	nullable := !required || schemaAllowsNull(schema)
	updatable := false
	column := &providerv1.ColumnMetadata{
		Name:          name,
		SourceName:    name,
		Type:          valueType,
		NativeType:    nativeType,
		Nullable:      &nullable,
		Updatable:     &updatable,
		Searchability: providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
		Annotation:    schema.Description,
		Properties:    make(map[string]string),
	}
	if schema.Format != "" {
		column.Properties["openapi.format"] = schema.Format
	}
	if schema.ReadOnly != nil {
		column.Properties["openapi.read_only"] = strconv.FormatBool(*schema.ReadOnly)
	}
	if schema.WriteOnly != nil {
		column.Properties["openapi.write_only"] = strconv.FormatBool(*schema.WriteOnly)
	}
	if schema.MaxLength != nil && *schema.MaxLength >= 0 && *schema.MaxLength <= math.MaxInt32 {
		length := int32(*schema.MaxLength)
		column.Length = &length
	}
	return column, nil
}

func schemaValueType(schema *base.Schema) (kublingv1.ValueType, string) {
	types := nonNullSchemaTypes(schema)
	if len(types) != 1 {
		return kublingv1.ValueType_VALUE_TYPE_JSON, strings.Join(types, "|")
	}
	typeName := types[0]
	nativeType := typeName
	if schema.Format != "" {
		nativeType += "(" + schema.Format + ")"
	}
	switch typeName {
	case "boolean":
		return kublingv1.ValueType_VALUE_TYPE_BOOLEAN, nativeType
	case "integer":
		switch strings.ToLower(schema.Format) {
		case "int32":
			return kublingv1.ValueType_VALUE_TYPE_INTEGER, nativeType
		case "int64":
			return kublingv1.ValueType_VALUE_TYPE_LONG, nativeType
		default:
			return kublingv1.ValueType_VALUE_TYPE_BIGINTEGER, nativeType
		}
	case "number":
		switch strings.ToLower(schema.Format) {
		case "float":
			return kublingv1.ValueType_VALUE_TYPE_FLOAT, nativeType
		case "double":
			return kublingv1.ValueType_VALUE_TYPE_DOUBLE, nativeType
		default:
			return kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL, nativeType
		}
	case "string":
		switch strings.ToLower(schema.Format) {
		case "date":
			return kublingv1.ValueType_VALUE_TYPE_DATE, nativeType
		case "time":
			return kublingv1.ValueType_VALUE_TYPE_TIME, nativeType
		case "date-time":
			return kublingv1.ValueType_VALUE_TYPE_TIMESTAMP, nativeType
		case "byte", "binary":
			return kublingv1.ValueType_VALUE_TYPE_VARBINARY, nativeType
		default:
			return kublingv1.ValueType_VALUE_TYPE_STRING, nativeType
		}
	default:
		return kublingv1.ValueType_VALUE_TYPE_JSON, nativeType
	}
}

func nonNullSchemaTypes(schema *base.Schema) []string {
	types := make(map[string]struct{})
	collectSchemaTypes(schema, types, make(map[*base.Schema]bool))
	delete(types, "null")
	result := make([]string, 0, len(types))
	for typeName := range types {
		result = append(result, typeName)
	}
	sort.Strings(result)
	return result
}

func collectSchemaTypes(schema *base.Schema, types map[string]struct{}, visiting map[*base.Schema]bool) {
	if schema == nil || visiting[schema] {
		return
	}
	visiting[schema] = true
	defer delete(visiting, schema)
	for _, typeName := range schema.Type {
		types[strings.ToLower(typeName)] = struct{}{}
	}
	if len(schema.Type) == 0 {
		if schema.Properties != nil || len(schema.AllOf) > 0 {
			types["object"] = struct{}{}
		}
		if schema.Items != nil {
			types["array"] = struct{}{}
		}
	}
	for _, alternative := range append(append([]*base.SchemaProxy(nil), schema.OneOf...), schema.AnyOf...) {
		collectSchemaTypes(alternative.Schema(), types, visiting)
	}
}

func hasSchemaType(schema *base.Schema, expected string) bool {
	for _, typeName := range schema.Type {
		if strings.EqualFold(typeName, expected) {
			return true
		}
	}
	return expected == "object" && schema.Properties != nil || expected == "array" && schema.Items != nil
}

func schemaAllowsNull(schema *base.Schema) bool {
	if schema.Nullable != nil && *schema.Nullable {
		return true
	}
	for _, typeName := range schema.Type {
		if strings.EqualFold(typeName, "null") {
			return true
		}
	}
	for _, alternative := range append(append([]*base.SchemaProxy(nil), schema.OneOf...), schema.AnyOf...) {
		alternativeSchema := alternative.Schema()
		if alternativeSchema != nil && schemaAllowsNull(alternativeSchema) {
			return true
		}
	}
	return false
}
