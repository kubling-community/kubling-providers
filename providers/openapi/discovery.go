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
	requiredParameters []*v3.Parameter
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
		if operationID == "" || strings.Contains(operationPath, "{") {
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
		if len(inspection.requiredParameters) > 0 {
			if _, explicitlyIncluded := includedOperations[lookupOperation]; explicitlyIncluded {
				return nil, fmt.Errorf("discover operation %q: required parameters need an explicit entity mapping", operationID)
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
		matchedIncludes[lookupOperation] = struct{}{}
		entities = append(entities, EntityConfig{
			Name:          discoveredEntityName(operationID, arrays[0]),
			ListOperation: operationID,
			ResponsePath:  arrays[0].responsePath,
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
	inspection.requiredParameters = requiredOperationParameters(pathItem, pathItem.Get)
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

func requiredOperationParameters(pathItem *v3.PathItem, operation *v3.Operation) []*v3.Parameter {
	var required []*v3.Parameter
	for _, parameter := range append(append([]*v3.Parameter(nil), pathItem.Parameters...), operation.Parameters...) {
		if parameter != nil && parameter.Required != nil && *parameter.Required {
			required = append(required, parameter)
		}
	}
	return required
}
