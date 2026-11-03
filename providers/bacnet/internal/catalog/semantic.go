package catalog

import (
	"fmt"
	"strings"
	"unicode"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"gopkg.in/yaml.v3"
)

const semanticFragmentVersion = "1.0.0"

type semanticDocument struct {
	Format        string           `yaml:"format"`
	SchemaVersion int              `yaml:"schemaVersion"`
	Kind          string           `yaml:"kind"`
	Metadata      semanticMetadata `yaml:"metadata"`
	Spec          semanticSpec     `yaml:"spec"`
}

type semanticMetadata struct {
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
	Version   string `yaml:"version"`
}

type semanticSpec struct {
	Entities      []semanticEntity       `yaml:"entities"`
	Relationships []semanticRelationship `yaml:"relationships"`
}

type semanticEntity struct {
	ID              string             `yaml:"id"`
	Name            string             `yaml:"name"`
	Description     string             `yaml:"description"`
	Binding         semanticBinding    `yaml:"binding"`
	Properties      []semanticProperty `yaml:"properties"`
	Identities      []semanticIdentity `yaml:"identities"`
	propertyByField map[string]string  `yaml:"-"`
}

type semanticBinding struct {
	Relation string `yaml:"relation,omitempty"`
	Field    string `yaml:"field,omitempty"`
}

type semanticProperty struct {
	ID      string          `yaml:"id"`
	Name    string          `yaml:"name"`
	Type    string          `yaml:"type"`
	Binding semanticBinding `yaml:"binding"`
}

type semanticIdentity struct {
	ID         string   `yaml:"id"`
	Properties []string `yaml:"properties"`
}

type semanticRelationship struct {
	ID          string         `yaml:"id"`
	Name        string         `yaml:"name"`
	Description string         `yaml:"description"`
	From        string         `yaml:"from"`
	To          string         `yaml:"to"`
	Cardinality string         `yaml:"cardinality"`
	Joins       []semanticJoin `yaml:"joins"`
}

type semanticJoin struct {
	Left  string `yaml:"left"`
	Right string `yaml:"right"`
}

type entityProfile struct {
	ID   string
	Name string
}

var entityProfiles = map[string]entityProfile{
	ScopesTable:           {ID: "Scope", Name: "BACnet scope"},
	DevicesTable:          {ID: "Device", Name: "BACnet device"},
	DeviceObjectsTable:    {ID: "DeviceObject", Name: "BACnet device object"},
	ObjectsTable:          {ID: "Object", Name: "BACnet object"},
	AnalogPointsTable:     {ID: "AnalogPoint", Name: "Analog point"},
	BinaryPointsTable:     {ID: "BinaryPoint", Name: "Binary point"},
	MultistatePointsTable: {ID: "MultiStatePoint", Name: "Multi-state point"},
}

type relationshipDefinition struct {
	ID          string
	Name        string
	Description string
	FromTable   string
	ToTable     string
	Cardinality string
	Columns     []string
}

func buildSemanticFragment(
	metadata *providerv1.SchemaMetadata,
) (*providersdk.SemanticFragment, error) {
	entities := make([]semanticEntity, 0, len(metadata.GetTables()))
	entitiesByTable := make(map[string]semanticEntity, len(metadata.GetTables()))
	seenIDs := make(map[string]string, len(metadata.GetTables()))
	for _, table := range metadata.GetTables() {
		profile := semanticEntityProfile(table.GetName())
		if previous, exists := seenIDs[profile.ID]; exists {
			return nil, fmt.Errorf(
				"BACnet semantic tables %q and %q map to entity %q",
				previous,
				table.GetName(),
				profile.ID,
			)
		}
		candidate, err := buildSemanticEntity(table, profile)
		if err != nil {
			return nil, err
		}
		seenIDs[profile.ID] = table.GetName()
		entities = append(entities, candidate)
		entitiesByTable[table.GetName()] = candidate
	}

	relationships, err := buildSemanticRelationships(metadata, entitiesByTable)
	if err != nil {
		return nil, err
	}
	document, err := yaml.Marshal(semanticDocument{
		Format:        "kubling-semantic",
		SchemaVersion: 1,
		Kind:          "fragment",
		Metadata: semanticMetadata{
			Name:      "bacnet-building-automation",
			Namespace: "bacnet",
			Version:   semanticFragmentVersion,
		},
		Spec: semanticSpec{
			Entities:      entities,
			Relationships: relationships,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal BACnet semantic fragment: %w", err)
	}
	return &providersdk.SemanticFragment{
		Document:  document,
		MediaType: providersdk.SemanticFragmentMediaTypeYAML,
		Version:   semanticFragmentVersion,
	}, nil
}

func semanticEntityProfile(tableName string) entityProfile {
	if configured, exists := entityProfiles[tableName]; exists {
		return configured
	}
	return entityProfile{
		ID:   upperCamel(tableName),
		Name: displayName(tableName),
	}
}

func buildSemanticEntity(
	table *providerv1.TableMetadata,
	profile entityProfile,
) (semanticEntity, error) {
	if table == nil {
		return semanticEntity{}, fmt.Errorf("BACnet semantic table is required")
	}
	properties := make([]semanticProperty, 0, len(table.GetColumns())-1)
	propertyByField := make(map[string]string, len(table.GetColumns())-1)
	seenPropertyIDs := make(map[string]string, len(table.GetColumns())-1)
	var stableKey []string
	for _, column := range table.GetColumns() {
		if column.GetStableKey() != nil {
			stableKey = append([]string(nil), column.GetStableKey().GetColumns()...)
			continue
		}
		propertyID := lowerCamel(column.GetName())
		if previous, exists := seenPropertyIDs[propertyID]; exists {
			return semanticEntity{}, fmt.Errorf(
				"BACnet semantic entity %s fields %q and %q map to property %q",
				profile.ID,
				previous,
				column.GetName(),
				propertyID,
			)
		}
		semanticType, err := semanticType(column)
		if err != nil {
			return semanticEntity{}, fmt.Errorf(
				"BACnet semantic field %s.%s: %w",
				table.GetName(),
				column.GetName(),
				err,
			)
		}
		properties = append(properties, semanticProperty{
			ID:   propertyID,
			Name: displayName(column.GetName()),
			Type: semanticType,
			Binding: semanticBinding{
				Field: column.GetName(),
			},
		})
		seenPropertyIDs[propertyID] = column.GetName()
		propertyByField[column.GetName()] = propertyID
	}
	if len(stableKey) == 0 {
		return semanticEntity{}, fmt.Errorf("BACnet semantic entity %s has no stable identity", profile.ID)
	}
	identityProperties := make([]string, 0, len(stableKey))
	for _, field := range stableKey {
		propertyID, exists := propertyByField[field]
		if !exists {
			return semanticEntity{}, fmt.Errorf(
				"BACnet semantic identity %s references unknown field %q",
				profile.ID,
				field,
			)
		}
		identityProperties = append(identityProperties, propertyID)
	}

	return semanticEntity{
		ID:          profile.ID,
		Name:        profile.Name,
		Description: table.GetAnnotation(),
		Binding: semanticBinding{
			Relation: table.GetName(),
		},
		Properties: properties,
		Identities: []semanticIdentity{{
			ID:         lowerFirst(profile.ID) + "Identity",
			Properties: identityProperties,
		}},
		propertyByField: propertyByField,
	}, nil
}

func semanticType(column *providerv1.ColumnMetadata) (string, error) {
	descriptor := column.GetTypeDescriptor()
	if descriptor == nil {
		descriptor = &kublingv1.TypeDescriptor{Type: column.GetType()}
	}
	if descriptor.GetType() == kublingv1.ValueType_VALUE_TYPE_ARRAY {
		if descriptor.GetElementType() == nil {
			return "", fmt.Errorf("ARRAY column has no element type")
		}
		element, err := scalarSemanticType(descriptor.GetElementType().GetType())
		if err != nil {
			return "", err
		}
		return element + "[]", nil
	}
	return scalarSemanticType(descriptor.GetType())
}

func scalarSemanticType(valueType kublingv1.ValueType) (string, error) {
	switch valueType {
	case kublingv1.ValueType_VALUE_TYPE_STRING:
		return "string", nil
	case kublingv1.ValueType_VALUE_TYPE_VARBINARY:
		return "varbinary", nil
	case kublingv1.ValueType_VALUE_TYPE_BOOLEAN:
		return "boolean", nil
	case kublingv1.ValueType_VALUE_TYPE_BYTE:
		return "byte", nil
	case kublingv1.ValueType_VALUE_TYPE_SHORT:
		return "short", nil
	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		return "integer", nil
	case kublingv1.ValueType_VALUE_TYPE_LONG:
		return "long", nil
	case kublingv1.ValueType_VALUE_TYPE_BIGINTEGER:
		return "biginteger", nil
	case kublingv1.ValueType_VALUE_TYPE_FLOAT:
		return "float", nil
	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		return "double", nil
	case kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL:
		return "bigdecimal", nil
	case kublingv1.ValueType_VALUE_TYPE_DATE:
		return "date", nil
	case kublingv1.ValueType_VALUE_TYPE_TIME:
		return "time", nil
	case kublingv1.ValueType_VALUE_TYPE_TIMESTAMP:
		return "timestamp", nil
	case kublingv1.ValueType_VALUE_TYPE_JSON:
		return "json", nil
	default:
		return "", fmt.Errorf("unsupported semantic value type %s", valueType)
	}
}

func buildSemanticRelationships(
	metadata *providerv1.SchemaMetadata,
	entities map[string]semanticEntity,
) ([]semanticRelationship, error) {
	definitions := []relationshipDefinition{
		{
			ID:          "ScopeHasDevices",
			Name:        "Scope has devices",
			Description: "A logical BACnet scope contains the devices selected by its current registry snapshot.",
			FromTable:   ScopesTable,
			ToTable:     DevicesTable,
			Cardinality: "oneToMany",
			Columns:     []string{NamespaceColumn, ScopeIDColumn},
		},
		{
			ID:          "DeviceHasObjects",
			Name:        "Device has objects",
			Description: "A BACnet device exposes object instances through its selected profile.",
			FromTable:   DevicesTable,
			ToTable:     ObjectsTable,
			Cardinality: "oneToMany",
			Columns:     []string{NamespaceColumn, ScopeIDColumn, DeviceInstanceColumn},
		},
	}
	for _, table := range metadata.GetTables() {
		switch table.GetName() {
		case ScopesTable, DevicesTable, ObjectsTable:
			continue
		}
		entity := entities[table.GetName()]
		definitions = append(definitions, relationshipDefinition{
			ID:          "ObjectHas" + entity.ID,
			Name:        "Object has " + strings.ToLower(entity.Name),
			Description: "A specialized BACnet projection describes the same object instance.",
			FromTable:   ObjectsTable,
			ToTable:     table.GetName(),
			Cardinality: "oneToOne",
			Columns:     append([]string(nil), objectIdentityColumns...),
		})
	}

	result := make([]semanticRelationship, 0, len(definitions))
	seenIDs := make(map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		if _, exists := seenIDs[definition.ID]; exists {
			return nil, fmt.Errorf("duplicate BACnet semantic relationship %q", definition.ID)
		}
		seenIDs[definition.ID] = struct{}{}
		from, fromExists := entities[definition.FromTable]
		to, toExists := entities[definition.ToTable]
		if !fromExists || !toExists {
			return nil, fmt.Errorf("relationship %s references an unknown BACnet entity", definition.ID)
		}
		joins := make([]semanticJoin, 0, len(definition.Columns))
		for _, field := range definition.Columns {
			left, leftExists := from.propertyByField[field]
			right, rightExists := to.propertyByField[field]
			if !leftExists || !rightExists {
				return nil, fmt.Errorf(
					"relationship %s references unknown field %q",
					definition.ID,
					field,
				)
			}
			joins = append(joins, semanticJoin{
				Left:  from.ID + "." + left,
				Right: to.ID + "." + right,
			})
		}
		result = append(result, semanticRelationship{
			ID:          definition.ID,
			Name:        definition.Name,
			Description: definition.Description,
			From:        from.ID,
			To:          to.ID,
			Cardinality: definition.Cardinality,
			Joins:       joins,
		})
	}
	return result, nil
}

func lowerCamel(value string) string {
	parts := strings.Split(strings.ToLower(value), "_")
	for index := 1; index < len(parts); index++ {
		parts[index] = upperFirst(parts[index])
	}
	return strings.Join(parts, "")
}

func upperCamel(value string) string {
	parts := strings.Split(strings.ToLower(value), "_")
	for index := range parts {
		parts[index] = upperFirst(parts[index])
	}
	return strings.Join(parts, "")
}

func displayName(value string) string {
	aliases := map[string]string{
		"apdu":   "APDU",
		"bacnet": "BACnet",
		"id":     "ID",
	}
	parts := strings.Split(strings.ToLower(value), "_")
	for index, part := range parts {
		if alias, exists := aliases[part]; exists {
			parts[index] = alias
			continue
		}
		if index == 0 {
			parts[index] = upperFirst(part)
		}
	}
	return strings.Join(parts, " ")
}

func lowerFirst(value string) string {
	if value == "" {
		return value
	}
	runes := []rune(value)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

func upperFirst(value string) string {
	if value == "" {
		return value
	}
	runes := []rune(value)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}
