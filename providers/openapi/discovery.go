package openapi

import (
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/pb33f/libopenapi/datamodel/high/base"
	v3 "github.com/pb33f/libopenapi/datamodel/high/v3"
)

type discoveredArray struct {
	responsePath string
	itemSchema   *base.SchemaProxy
}

type getOperationInspection struct {
	path               string
	pathItem           *v3.PathItem
	operation          *v3.Operation
	operationID        string
	responseSchema     *base.Schema
	arrays             []discoveredArray
	pathParameters     []PathParameterConfig
	requiredParameters []*v3.Parameter
	bindingErr         error
	err                error
}

func discoverEntities(document *v3.Document, discovery *DiscoveryConfig, explicit []EntityConfig) ([]EntityConfig, error) {
	if discovery == nil || !discovery.Enabled {
		return nil, nil
	}
	if document == nil || document.Paths == nil || document.Paths.PathItems == nil {
		return nil, nil
	}
	explicitOperations := make(map[string]struct{}, len(explicit))
	for _, entity := range explicit {
		explicitOperations[strings.ToUpper(entity.ListOperation)] = struct{}{}
	}
	includedOperations := uppercaseSet(discovery.IncludeOperations)
	excludedOperations := uppercaseSet(discovery.ExcludeOperations)
	includedTags := uppercaseSet(discovery.IncludeTags)
	matchedIncludes := make(map[string]struct{}, len(includedOperations))

	var entities []EntityConfig
	for operationPath, pathItem := range document.Paths.PathItems.FromOldest() {
		if pathItem == nil || pathItem.Get == nil {
			continue
		}
		inspection := inspectGETOperation(operationPath, pathItem)
		operation := inspection.operation
		operationID := inspection.operationID
		lookupOperation := strings.ToUpper(operationID)
		if operationID == "" {
			continue
		}
		if _, exists := explicitOperations[lookupOperation]; exists {
			matchedIncludes[lookupOperation] = struct{}{}
			continue
		}
		if _, excluded := excludedOperations[lookupOperation]; excluded {
			continue
		}
		if len(includedOperations) > 0 {
			if _, included := includedOperations[lookupOperation]; !included {
				continue
			}
		}
		if len(includedTags) > 0 && !operationHasTag(operation, includedTags) {
			continue
		}
		if inspection.bindingErr != nil {
			if _, explicitlyIncluded := includedOperations[lookupOperation]; explicitlyIncluded {
				return nil, fmt.Errorf("discover operation %q: %w", operationID, inspection.bindingErr)
			}
			continue
		}
		if len(inspection.requiredParameters) > 0 {
			if _, explicitlyIncluded := includedOperations[lookupOperation]; explicitlyIncluded {
				return nil, fmt.Errorf(
					"discover operation %q: required non-path parameters need an explicit entity mapping",
					operationID,
				)
			}
			continue
		}
		if inspection.err != nil {
			if _, explicitlyIncluded := includedOperations[lookupOperation]; explicitlyIncluded {
				return nil, fmt.Errorf("discover operation %q: %w", operationID, inspection.err)
			}
			continue
		}
		arrays := inspection.arrays
		if len(arrays) != 1 {
			if _, explicitlyIncluded := includedOperations[lookupOperation]; explicitlyIncluded {
				return nil, fmt.Errorf("discover operation %q: found %d candidate response arrays, want exactly one", operationID, len(arrays))
			}
			continue
		}
		if err := validateDiscoveredPathFields(arrays[0], inspection.pathParameters); err != nil {
			if _, explicitlyIncluded := includedOperations[lookupOperation]; explicitlyIncluded {
				return nil, fmt.Errorf("discover operation %q: %w", operationID, err)
			}
			continue
		}
		matchedIncludes[lookupOperation] = struct{}{}
		entities = append(entities, EntityConfig{
			Name:           discoveredEntityName(operationID, arrays[0]),
			ListOperation:  operationID,
			ResponsePath:   arrays[0].responsePath,
			PathParameters: clonePathParameters(inspection.pathParameters),
		})
	}

	for _, operation := range discovery.IncludeOperations {
		if _, matched := matchedIncludes[strings.ToUpper(operation)]; !matched {
			return nil, fmt.Errorf("discovery includeOperation %q was not found as an eligible GET operation", operation)
		}
	}
	return entities, nil
}

func inspectGETOperation(operationPath string, pathItem *v3.PathItem) getOperationInspection {
	inspection := getOperationInspection{path: operationPath, pathItem: pathItem}
	if pathItem == nil || pathItem.Get == nil {
		inspection.err = fmt.Errorf("GET operation is missing")
		return inspection
	}
	inspection.operation = pathItem.Get
	inspection.operationID = strings.TrimSpace(pathItem.Get.OperationId)
	inspection.pathParameters, inspection.requiredParameters, inspection.bindingErr = inspectListRequestBindings(
		operationPath,
		pathItem,
		pathItem.Get,
	)
	inspection.responseSchema, inspection.err = successfulJSONResponseSchema(pathItem.Get)
	if inspection.err != nil {
		return inspection
	}
	inspection.arrays, inspection.err = discoverObjectArrays(
		inspection.responseSchema,
		"",
		make(map[*base.Schema]bool),
	)
	return inspection
}

func inspectListRequestBindings(
	operationPath string,
	pathItem *v3.PathItem,
	operation *v3.Operation,
) ([]PathParameterConfig, []*v3.Parameter, error) {
	parameters, err := effectiveOperationParameters(pathItem, operation)
	if err != nil {
		return nil, nil, err
	}
	declaredPath := make(map[string]*v3.Parameter)
	var requiredNonPath []*v3.Parameter
	for _, parameter := range parameters {
		location := strings.ToLower(strings.TrimSpace(parameter.In))
		if location == "path" {
			declaredPath[strings.TrimSpace(parameter.Name)] = parameter
			continue
		}
		if parameter.Required != nil && *parameter.Required {
			requiredNonPath = append(requiredNonPath, parameter)
		}
	}

	names, err := operationPathParameterNames(operationPath)
	if err != nil {
		return nil, nil, err
	}
	bindings := make([]PathParameterConfig, 0, len(names))
	seen := make(map[string]struct{}, len(names))
	fields := make(map[string]string, len(names))
	for _, name := range names {
		if _, exists := seen[name]; exists {
			continue
		}
		parameter := declaredPath[name]
		if parameter == nil {
			return nil, nil, fmt.Errorf(
				"path parameter %q is present in operation path %q but is not defined",
				name,
				operationPath,
			)
		}
		if err := validatePathParameter(parameter); err != nil {
			return nil, nil, fmt.Errorf("path parameter %q: %w", name, err)
		}
		lookupField := strings.ToUpper(name)
		if existing := fields[lookupField]; existing != "" && existing != name {
			return nil, nil, fmt.Errorf(
				"path parameters %q and %q cannot be represented as distinct case-insensitive fields",
				existing,
				name,
			)
		}
		seen[name] = struct{}{}
		fields[lookupField] = name
		bindings = append(bindings, PathParameterConfig{Parameter: name, Field: name})
	}

	undeclared := make([]string, 0)
	for name := range declaredPath {
		if _, exists := seen[name]; !exists {
			undeclared = append(undeclared, name)
		}
	}
	if len(undeclared) > 0 {
		sort.Strings(undeclared)
		return nil, nil, fmt.Errorf(
			"path parameter %q is defined but is not present in operation path %q",
			undeclared[0],
			operationPath,
		)
	}
	return bindings, requiredNonPath, nil
}

func operationPathParameterNames(operationPath string) ([]string, error) {
	var names []string
	for position := 0; position < len(operationPath); {
		openOffset := strings.IndexByte(operationPath[position:], '{')
		if openOffset < 0 {
			if strings.Contains(operationPath[position:], "}") {
				return nil, fmt.Errorf("operation path %q contains an unmatched }", operationPath)
			}
			break
		}
		open := position + openOffset
		if strings.Contains(operationPath[position:open], "}") {
			return nil, fmt.Errorf("operation path %q contains an unmatched }", operationPath)
		}
		closeOffset := strings.IndexByte(operationPath[open+1:], '}')
		if closeOffset < 0 {
			return nil, fmt.Errorf("operation path %q contains an unclosed path parameter", operationPath)
		}
		close := open + 1 + closeOffset
		name := operationPath[open+1 : close]
		if name == "" || strings.ContainsAny(name, "{}/") {
			return nil, fmt.Errorf("operation path %q contains invalid path parameter %q", operationPath, name)
		}
		names = append(names, name)
		position = close + 1
	}
	return names, nil
}

func validateDiscoveredPathFields(candidate discoveredArray, bindings []PathParameterConfig) error {
	if len(bindings) == 0 {
		return nil
	}
	rowSchema := candidate.itemSchema.Schema()
	if rowSchema == nil {
		return fmt.Errorf("build array item schema: %w", candidate.itemSchema.GetBuildError())
	}
	properties, _, err := objectShape(rowSchema, make(map[*base.Schema]bool))
	if err != nil {
		return err
	}
	responseFields := make(map[string]string, len(properties))
	for name := range properties {
		responseFields[strings.ToUpper(name)] = name
	}
	for _, binding := range bindings {
		if responseField := responseFields[strings.ToUpper(binding.Field)]; responseField != "" {
			return fmt.Errorf(
				"path parameter %q would share field %q with response property %q; use an explicit entity mapping",
				binding.Parameter,
				binding.Field,
				responseField,
			)
		}
	}
	return nil
}

func discoverObjectArrays(schema *base.Schema, pointer string, visiting map[*base.Schema]bool) ([]discoveredArray, error) {
	if schema == nil || visiting[schema] {
		return nil, nil
	}
	if hasSchemaType(schema, "array") {
		if schema.Items == nil || !schema.Items.IsA() || schema.Items.A == nil {
			return nil, nil
		}
		rowSchema := schema.Items.A.Schema()
		if rowSchema == nil {
			return nil, fmt.Errorf("build array item schema: %w", schema.Items.A.GetBuildError())
		}
		if _, _, err := objectShape(rowSchema, make(map[*base.Schema]bool)); err != nil {
			return nil, nil
		}
		return []discoveredArray{{responsePath: pointer, itemSchema: schema.Items.A}}, nil
	}

	properties, _, err := objectShape(schema, make(map[*base.Schema]bool))
	if err != nil {
		return nil, nil
	}
	visiting[schema] = true
	defer delete(visiting, schema)
	names := make([]string, 0, len(properties))
	for name := range properties {
		names = append(names, name)
	}
	sort.Strings(names)
	var arrays []discoveredArray
	for _, name := range names {
		property := properties[name]
		propertySchema := property.Schema()
		if propertySchema == nil {
			return nil, fmt.Errorf("build response property %q: %w", name, property.GetBuildError())
		}
		discovered, err := discoverObjectArrays(propertySchema, pointer+"/"+escapeJSONPointerToken(name), visiting)
		if err != nil {
			return nil, err
		}
		arrays = append(arrays, discovered...)
	}
	return arrays, nil
}

func discoveredEntityName(operationID string, candidate discoveredArray) string {
	if reference := candidate.itemSchema.GetReference(); reference != "" {
		parts := strings.Split(reference, "/")
		if name := relationalIdentifier(parts[len(parts)-1]); name != "" {
			return name
		}
	}
	if schema := candidate.itemSchema.Schema(); schema != nil {
		if name := relationalIdentifier(schema.Title); name != "" {
			return name
		}
	}
	if candidate.responsePath != "" {
		parts, _ := parseJSONPointer(candidate.responsePath)
		if name := relationalIdentifier(parts[len(parts)-1]); name != "" {
			return name
		}
	}
	return relationalIdentifier(operationID)
}

func relationalIdentifier(value string) string {
	var result strings.Builder
	runes := []rune(strings.TrimSpace(value))
	for index, current := range runes {
		if !unicode.IsLetter(current) && !unicode.IsDigit(current) {
			if result.Len() > 0 && !strings.HasSuffix(result.String(), "_") {
				result.WriteByte('_')
			}
			continue
		}
		if unicode.IsUpper(current) && index > 0 && result.Len() > 0 {
			previous := runes[index-1]
			nextIsLower := index+1 < len(runes) && unicode.IsLower(runes[index+1])
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || unicode.IsUpper(previous) && nextIsLower {
				if !strings.HasSuffix(result.String(), "_") {
					result.WriteByte('_')
				}
			}
		}
		result.WriteRune(unicode.ToUpper(current))
	}
	return strings.Trim(result.String(), "_")
}

func escapeJSONPointerToken(token string) string {
	return strings.ReplaceAll(strings.ReplaceAll(token, "~", "~0"), "/", "~1")
}

func uppercaseSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[strings.ToUpper(value)] = struct{}{}
	}
	return result
}

func operationHasTag(operation *v3.Operation, included map[string]struct{}) bool {
	for _, tag := range operation.Tags {
		if _, exists := included[strings.ToUpper(tag)]; exists {
			return true
		}
	}
	return false
}

func clonePathParameters(parameters []PathParameterConfig) []PathParameterConfig {
	return append([]PathParameterConfig(nil), parameters...)
}
