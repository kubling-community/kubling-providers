package openapi

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/pb33f/libopenapi/datamodel/high/base"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
	"gopkg.in/yaml.v3"
)

type configTemplateCandidate struct {
	entity  EntityConfig
	comment string
}

type configTemplateEntity struct {
	Name            string                         `yaml:"name"`
	ListOperation   string                         `yaml:"listOperation"`
	ResponsePath    string                         `yaml:"responsePath,omitempty"`
	PrimaryKey      []string                       `yaml:"primaryKey"`
	QueryParameters []configTemplateQueryParameter `yaml:"queryParameters,omitempty"`
	Pagination      *configTemplatePagination      `yaml:"pagination,omitempty"`
}

type configTemplateQueryParameter struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

type configTemplatePagination struct {
	Mode              PaginationMode `yaml:"mode"`
	PageSize          uint32         `yaml:"pageSize"`
	PageSizeParameter string         `yaml:"pageSizeParameter,omitempty"`
	CursorParameter   string         `yaml:"cursorParameter"`
	NextCursorPath    string         `yaml:"nextCursorPath"`
	HasMorePath       string         `yaml:"hasMorePath,omitempty"`
}

func GenerateConfigTemplate(path string) ([]byte, error) {
	config, err := loadConfig(path, false)
	if err != nil {
		return nil, err
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{}
	}
	document, err := loadDocument(config.SpecFile, config.SpecHeaders, client, config.RequestTimeout)
	if err != nil {
		return nil, err
	}
	candidates, diagnostics := inspectConfigTemplate(document)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("OpenAPI document contains no configurable GET entity candidates")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read OpenAPI provider config template source: %w", err)
	}
	return renderConfigTemplate(raw, candidates, diagnostics)
}

func inspectConfigTemplate(document *v3.Document) ([]configTemplateCandidate, []string) {
	if document == nil || document.Paths == nil || document.Paths.PathItems == nil {
		return nil, []string{"OpenAPI document defines no paths"}
	}
	var candidates []configTemplateCandidate
	var diagnostics []string
	for operationPath, pathItem := range document.Paths.PathItems.FromOldest() {
		if pathItem == nil || pathItem.Get == nil {
			continue
		}
		inspection := inspectGETOperation(operationPath, pathItem)
		operationLabel := inspection.operationID
		if operationLabel == "" {
			operationLabel = "GET " + operationPath
			diagnostics = append(diagnostics, operationLabel+": operationId is required")
			continue
		}
		if strings.Contains(operationPath, "{") {
			diagnostics = append(diagnostics, operationLabel+": path parameters are not supported by list entities")
			continue
		}
		if inspection.err != nil {
			diagnostics = append(diagnostics, operationLabel+": "+inspection.err.Error())
			continue
		}
		if len(inspection.arrays) == 0 {
			diagnostics = append(diagnostics, operationLabel+": successful JSON response contains no object arrays")
			continue
		}
		queryParameters, unsupported := requiredTemplateQueryParameters(inspection)
		if unsupported != "" {
			diagnostics = append(diagnostics, operationLabel+": "+unsupported)
			continue
		}
		pagination := inferTemplatePagination(inspection)
		for _, array := range inspection.arrays {
			entity := EntityConfig{
				Name:            discoveredEntityName(inspection.operationID, array),
				ListOperation:   inspection.operationID,
				ResponsePath:    array.responsePath,
				PrimaryKey:      []string{},
				QueryParameters: cloneQueryParameters(queryParameters),
				Pagination:      clonePagination(pagination),
			}
			comment := fmt.Sprintf("Detected from GET %s. Review primaryKey", operationPath)
			if len(queryParameters) > 0 {
				comment += " and required parameter placeholders"
			}
			if pagination != nil {
				comment += "; pagination was inferred"
			}
			comment += "."
			candidates = append(candidates, configTemplateCandidate{
				entity:  entity,
				comment: comment,
			})
		}
	}
	assignUniqueTemplateNames(candidates)
	return candidates, diagnostics
}

func requiredTemplateQueryParameters(inspection getOperationInspection) ([]QueryParameterConfig, string) {
	parameters := make([]QueryParameterConfig, 0, len(inspection.requiredParameters))
	for _, parameter := range inspection.requiredParameters {
		name := strings.TrimSpace(parameter.Name)
		switch strings.ToLower(strings.TrimSpace(parameter.In)) {
		case "query":
			parameters = append(parameters, QueryParameterConfig{
				Name:  name,
				Value: "${OPENAPI_" + relationalIdentifier(inspection.operationID) + "_" + relationalIdentifier(name) + "}",
			})
		default:
			return nil, fmt.Sprintf("required %s parameter %q cannot be represented by an entity mapping", parameter.In, name)
		}
	}
	sort.Slice(parameters, func(left, right int) bool {
		return parameters[left].Name < parameters[right].Name
	})
	return parameters, ""
}

func inferTemplatePagination(inspection getOperationInspection) *PaginationConfig {
	queryParameters, err := operationQueryParameters(inspection.pathItem, inspection.operation)
	if err != nil {
		return nil
	}
	type cursorPattern struct {
		parameterNames []string
		cursorNames    []string
	}
	patterns := []cursorPattern{
		{parameterNames: []string{"page"}, cursorNames: []string{"next_page", "nextPage"}},
		{parameterNames: []string{"after"}, cursorNames: []string{"last_id", "lastId"}},
		{parameterNames: []string{"cursor", "after"}, cursorNames: []string{"next_cursor", "nextCursor", "next"}},
	}
	for _, pattern := range patterns {
		parameter := firstExistingParameter(queryParameters, pattern.parameterNames)
		if parameter == "" {
			continue
		}
		cursorPath := findObjectPropertyPointer(inspection.responseSchema, pattern.cursorNames)
		if cursorPath == "" {
			continue
		}
		pagination := &PaginationConfig{
			Mode:            PaginationModeCursor,
			PageSize:        20,
			CursorParameter: parameter,
			NextCursorPath:  cursorPath,
			HasMorePath: findObjectPropertyPointer(
				inspection.responseSchema,
				[]string{"has_more", "hasMore"},
			),
		}
		if queryParameters["limit"] != nil {
			pagination.PageSizeParameter = "limit"
		}
		return pagination
	}
	return nil
}

func firstExistingParameter(parameters map[string]*v3.Parameter, names []string) string {
	for _, name := range names {
		if parameters[name] != nil {
			return name
		}
	}
	return ""
}

func findObjectPropertyPointer(schema *base.Schema, preferredNames []string) string {
	return findObjectPropertyPointerAt(schema, "", preferredNames, make(map[*base.Schema]bool))
}

func findObjectPropertyPointerAt(
	schema *base.Schema,
	pointer string,
	preferredNames []string,
	visiting map[*base.Schema]bool,
) string {
	if schema == nil || visiting[schema] || hasSchemaType(schema, "array") {
		return ""
	}
	properties, _, err := objectShape(schema, make(map[*base.Schema]bool))
	if err != nil {
		return ""
	}
	for _, name := range preferredNames {
		if properties[name] != nil {
			return pointer + "/" + escapeJSONPointerToken(name)
		}
	}
	visiting[schema] = true
	defer delete(visiting, schema)
	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		propertySchema := properties[name].Schema()
		if result := findObjectPropertyPointerAt(
			propertySchema,
			pointer+"/"+escapeJSONPointerToken(name),
			preferredNames,
			visiting,
		); result != "" {
			return result
		}
	}
	return ""
}

func assignUniqueTemplateNames(candidates []configTemplateCandidate) {
	used := make(map[string]struct{}, len(candidates))
	for index := range candidates {
		baseName := candidates[index].entity.Name
		if baseName == "" {
			baseName = relationalIdentifier(candidates[index].entity.ListOperation)
		}
		name := baseName
		if _, exists := used[strings.ToUpper(name)]; exists {
			name = baseName + "_" + relationalIdentifier(candidates[index].entity.ListOperation)
		}
		for suffix := 2; ; suffix++ {
			lookup := strings.ToUpper(name)
			if _, exists := used[lookup]; !exists {
				used[lookup] = struct{}{}
				candidates[index].entity.Name = name
				break
			}
			name = fmt.Sprintf("%s_%d", baseName, suffix)
		}
	}
}

func cloneQueryParameters(parameters []QueryParameterConfig) []QueryParameterConfig {
	return append([]QueryParameterConfig(nil), parameters...)
}

func clonePagination(pagination *PaginationConfig) *PaginationConfig {
	if pagination == nil {
		return nil
	}
	copyPagination := *pagination
	copyPagination.StartPage = cloneUint64(pagination.StartPage)
	return &copyPagination
}

func renderConfigTemplate(
	raw []byte,
	candidates []configTemplateCandidate,
	diagnostics []string,
) ([]byte, error) {
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("decode OpenAPI provider config template source: %w", err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return nil, fmt.Errorf("OpenAPI provider config must be one YAML mapping")
	}
	entities := make([]configTemplateEntity, len(candidates))
	for index, candidate := range candidates {
		queryParameters := make([]configTemplateQueryParameter, len(candidate.entity.QueryParameters))
		for parameterIndex, parameter := range candidate.entity.QueryParameters {
			queryParameters[parameterIndex] = configTemplateQueryParameter{Name: parameter.Name, Value: parameter.Value}
		}
		var pagination *configTemplatePagination
		if candidate.entity.Pagination != nil {
			pagination = &configTemplatePagination{
				Mode:              candidate.entity.Pagination.Mode,
				PageSize:          candidate.entity.Pagination.PageSize,
				PageSizeParameter: candidate.entity.Pagination.PageSizeParameter,
				CursorParameter:   candidate.entity.Pagination.CursorParameter,
				NextCursorPath:    candidate.entity.Pagination.NextCursorPath,
				HasMorePath:       candidate.entity.Pagination.HasMorePath,
			}
		}
		entities[index] = configTemplateEntity{
			Name:            candidate.entity.Name,
			ListOperation:   candidate.entity.ListOperation,
			ResponsePath:    candidate.entity.ResponsePath,
			PrimaryKey:      []string{},
			QueryParameters: queryParameters,
			Pagination:      pagination,
		}
	}
	var entitiesNode yaml.Node
	if err := entitiesNode.Encode(entities); err != nil {
		return nil, fmt.Errorf("encode generated OpenAPI entities: %w", err)
	}
	comments := []string{
		"Generated from the OpenAPI document. Review entity names, primary keys,",
		"required parameter placeholders, pagination and mutation semantics before use.",
	}
	for _, diagnostic := range diagnostics {
		comments = append(comments, "Skipped: "+diagnostic)
	}
	for index, candidate := range candidates {
		comment := candidate.comment
		if index == 0 {
			comment = strings.Join(comments, "\n") + "\n" + comment
		}
		entitiesNode.Content[index].HeadComment = comment
	}
	setYAMLMappingValue(document.Content[0], "entities", &entitiesNode)

	var output bytes.Buffer
	encoder := yaml.NewEncoder(&output)
	encoder.SetIndent(2)
	if err := encoder.Encode(&document); err != nil {
		return nil, fmt.Errorf("render generated OpenAPI config: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return nil, fmt.Errorf("close generated OpenAPI config encoder: %w", err)
	}
	return output.Bytes(), nil
}

func setYAMLMappingValue(mapping *yaml.Node, key string, value *yaml.Node) {
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			mapping.Content[index+1] = value
			return
		}
	}
	mapping.Content = append(mapping.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
		value,
	)
}
