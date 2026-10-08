package semanticmodel

import (
	"fmt"
	"strings"
	"unicode"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-providers/providers/host/internal/hostschema"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"gopkg.in/yaml.v3"
)

const (
	Name      = "host-observability"
	Namespace = "host"
	Version   = "1.0.0"
)

type document struct {
	Format        string           `yaml:"format"`
	SchemaVersion int              `yaml:"schemaVersion"`
	Kind          string           `yaml:"kind"`
	Metadata      documentMetadata `yaml:"metadata"`
	Spec          documentSpec     `yaml:"spec"`
}

type documentMetadata struct {
	Name      string `yaml:"name"`
	Namespace string `yaml:"namespace"`
	Version   string `yaml:"version"`
}

type documentSpec struct {
	Entities      []entity       `yaml:"entities"`
	Relationships []relationship `yaml:"relationships"`
}

type entity struct {
	ID              string            `yaml:"id"`
	Name            string            `yaml:"name"`
	Description     string            `yaml:"description"`
	Binding         binding           `yaml:"binding"`
	Properties      []property        `yaml:"properties"`
	Identities      []identity        `yaml:"identities"`
	propertyByField map[string]string `yaml:"-"`
}

type binding struct {
	Relation string `yaml:"relation,omitempty"`
	Field    string `yaml:"field,omitempty"`
}

type property struct {
	ID      string  `yaml:"id"`
	Name    string  `yaml:"name"`
	Type    string  `yaml:"type"`
	Binding binding `yaml:"binding"`
}

type identity struct {
	ID         string   `yaml:"id"`
	Properties []string `yaml:"properties"`
}

type relationship struct {
	ID          string          `yaml:"id"`
	Name        string          `yaml:"name"`
	Description string          `yaml:"description"`
	From        string          `yaml:"from"`
	To          string          `yaml:"to"`
	Cardinality string          `yaml:"cardinality"`
	Joins       []joinCondition `yaml:"joins,omitempty"`
}

type joinCondition struct {
	Left  string `yaml:"left"`
	Right string `yaml:"right"`
}

type entityProfile struct {
	ID   string
	Name string
}

var entityProfiles = map[string]entityProfile{
	hostschema.HostTable:              {ID: "Host", Name: "Host"},
	hostschema.HostFactsTable:         {ID: "HostFacts", Name: "Host facts"},
	hostschema.CPUInfoTable:           {ID: "CPUInfo", Name: "CPU information"},
	hostschema.CPUTimesTable:          {ID: "CPUTimes", Name: "CPU times"},
	hostschema.MemoryTable:            {ID: "Memory", Name: "Memory"},
	hostschema.FilesystemsTable:       {ID: "Filesystem", Name: "Filesystem"},
	hostschema.NetworkInterfacesTable: {ID: "NetworkInterface", Name: "Network interface"},
	hostschema.NetworkIOTable:         {ID: "NetworkIO", Name: "Network I/O"},
	hostschema.ProcessesTable:         {ID: "Process", Name: "Process"},
}

type fieldJoin struct {
	Left  string
	Right string
}

type relationshipDefinition struct {
	ID          string
	Name        string
	Description string
	FromTable   string
	ToTable     string
	Cardinality string
	Joins       []fieldJoin
}

// Generate builds the exact bundled semantic document from the Host Provider's
// canonical metadata. Domain relationships remain explicit below so physical
// schema discovery never guesses meaning or cardinality.
func Generate() ([]byte, error) {
	metadata := hostschema.Metadata()
	entities := make([]entity, 0, len(metadata.GetTables()))
	entitiesByTable := make(map[string]entity, len(metadata.GetTables()))
	seenProfiles := make(map[string]struct{}, len(metadata.GetTables()))

	for _, table := range metadata.GetTables() {
		profile, exists := entityProfiles[table.GetName()]
		if !exists {
			return nil, fmt.Errorf("host semantic profile is missing table %q", table.GetName())
		}
		candidate, err := buildEntity(table, profile)
		if err != nil {
			return nil, err
		}
		entities = append(entities, candidate)
		entitiesByTable[table.GetName()] = candidate
		seenProfiles[table.GetName()] = struct{}{}
	}
	if len(seenProfiles) != len(entityProfiles) {
		return nil, fmt.Errorf(
			"host semantic profile defines %d tables but metadata exposes %d",
			len(entityProfiles),
			len(seenProfiles),
		)
	}

	relationships, err := buildRelationships(entitiesByTable)
	if err != nil {
		return nil, err
	}
	document := document{
		Format:        "kubling-semantic",
		SchemaVersion: 1,
		Kind:          "fragment",
		Metadata: documentMetadata{
			Name:      Name,
			Namespace: Namespace,
			Version:   Version,
		},
		Spec: documentSpec{
			Entities:      entities,
			Relationships: relationships,
		},
	}
	body, err := yaml.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("marshal host semantic fragment: %w", err)
	}
	return append(
		[]byte("# Generated from internal/hostschema; run go generate . and do not edit by hand.\n"),
		body...,
	), nil
}

func buildEntity(table *providerv1.TableMetadata, profile entityProfile) (entity, error) {
	properties := make([]property, 0, len(table.GetColumns())-1)
	propertyByField := make(map[string]string, len(table.GetColumns())-1)
	var stableKey []string
	for _, column := range table.GetColumns() {
		if column.GetName() == hostschema.IdentifierColumn {
			stableKey = column.GetStableKey().GetColumns()
			continue
		}
		propertyID := lowerCamel(column.GetName())
		if _, exists := propertyByField[column.GetName()]; exists {
			return entity{}, fmt.Errorf(
				"host semantic entity %s has duplicate field %q",
				profile.ID,
				column.GetName(),
			)
		}
		semanticType, err := semanticType(column)
		if err != nil {
			return entity{}, fmt.Errorf("%s.%s: %w", table.GetName(), column.GetName(), err)
		}
		properties = append(properties, property{
			ID:   propertyID,
			Name: displayName(column.GetName()),
			Type: semanticType,
			Binding: binding{
				Field: column.GetName(),
			},
		})
		propertyByField[column.GetName()] = propertyID
	}
	if len(stableKey) == 0 {
		return entity{}, fmt.Errorf("host semantic entity %s has no stable identity", profile.ID)
	}
	identityProperties := make([]string, 0, len(stableKey))
	for _, field := range stableKey {
		propertyID, exists := propertyByField[field]
		if !exists {
			return entity{}, fmt.Errorf(
				"host semantic identity %s references unknown field %q",
				profile.ID,
				field,
			)
		}
		identityProperties = append(identityProperties, propertyID)
	}

	return entity{
		ID:          profile.ID,
		Name:        profile.Name,
		Description: table.GetAnnotation(),
		Binding: binding{
			Relation: table.GetName(),
		},
		Properties: properties,
		Identities: []identity{{
			ID:         lowerFirst(profile.ID) + "Identity",
			Properties: identityProperties,
		}},
		propertyByField: propertyByField,
	}, nil
}

func semanticType(column *providerv1.ColumnMetadata) (string, error) {
	if column.GetType() == kublingv1.ValueType_VALUE_TYPE_ARRAY {
		descriptor := column.GetTypeDescriptor()
		if descriptor.GetType() != kublingv1.ValueType_VALUE_TYPE_ARRAY ||
			descriptor.GetElementType() == nil {
			return "", fmt.Errorf("ARRAY column has no element type")
		}
		element, err := scalarSemanticType(descriptor.GetElementType().GetType())
		if err != nil {
			return "", err
		}
		return element + "[]", nil
	}
	return scalarSemanticType(column.GetType())
}

func scalarSemanticType(valueType kublingv1.ValueType) (string, error) {
	switch valueType {
	case kublingv1.ValueType_VALUE_TYPE_STRING:
		return "string", nil
	case kublingv1.ValueType_VALUE_TYPE_INTEGER:
		return "integer", nil
	case kublingv1.ValueType_VALUE_TYPE_LONG:
		return "long", nil
	case kublingv1.ValueType_VALUE_TYPE_BIGINTEGER:
		return "biginteger", nil
	case kublingv1.ValueType_VALUE_TYPE_DOUBLE:
		return "double", nil
	case kublingv1.ValueType_VALUE_TYPE_TIMESTAMP:
		return "timestamp", nil
	default:
		return "", fmt.Errorf("unsupported semantic value type %s", valueType)
	}
}

func buildRelationships(entities map[string]entity) ([]relationship, error) {
	definitions := relationshipDefinitions()
	result := make([]relationship, 0, len(definitions))
	seen := make(map[string]struct{}, len(definitions))
	for _, definition := range definitions {
		if _, exists := seen[definition.ID]; exists {
			return nil, fmt.Errorf("duplicate host semantic relationship %q", definition.ID)
		}
		seen[definition.ID] = struct{}{}
		from, fromExists := entities[definition.FromTable]
		to, toExists := entities[definition.ToTable]
		if !fromExists || !toExists {
			return nil, fmt.Errorf("relationship %s references an unknown entity", definition.ID)
		}
		joins := make([]joinCondition, 0, len(definition.Joins))
		for _, candidate := range definition.Joins {
			left, leftExists := from.propertyByField[candidate.Left]
			right, rightExists := to.propertyByField[candidate.Right]
			if !leftExists || !rightExists {
				return nil, fmt.Errorf(
					"relationship %s references unknown fields %s.%s or %s.%s",
					definition.ID,
					definition.FromTable,
					candidate.Left,
					definition.ToTable,
					candidate.Right,
				)
			}
			joins = append(joins, joinCondition{
				Left:  from.ID + "." + left,
				Right: to.ID + "." + right,
			})
		}
		result = append(result, relationship{
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

func relationshipDefinitions() []relationshipDefinition {
	hostJoins := func() []fieldJoin {
		return []fieldJoin{{Left: "namespace", Right: "namespace"}, {Left: "host_id", Right: "host_id"}}
	}
	return []relationshipDefinition{
		{
			ID: "HostHasFacts", Name: "Host has facts",
			Description: "A registered host has at most one current operating-system facts row.",
			FromTable:   hostschema.HostTable, ToTable: hostschema.HostFactsTable,
			Cardinality: "oneToOne", Joins: hostJoins(),
		},
		{
			ID: "HostHasCPUInfo", Name: "Host has CPU information",
			Description: "A registered host reports its logical processor inventory.",
			FromTable:   hostschema.HostTable, ToTable: hostschema.CPUInfoTable,
			Cardinality: "oneToMany", Joins: hostJoins(),
		},
		{
			ID: "HostHasCPUTimes", Name: "Host has CPU times",
			Description: "A registered host reports cumulative per-processor CPU counters.",
			FromTable:   hostschema.HostTable, ToTable: hostschema.CPUTimesTable,
			Cardinality: "oneToMany", Joins: hostJoins(),
		},
		{
			ID: "HostHasMemory", Name: "Host has memory state",
			Description: "A registered host has at most one current memory observation.",
			FromTable:   hostschema.HostTable, ToTable: hostschema.MemoryTable,
			Cardinality: "oneToOne", Joins: hostJoins(),
		},
		{
			ID: "HostHasFilesystems", Name: "Host has filesystems",
			Description: "A registered host reports its mounted filesystems.",
			FromTable:   hostschema.HostTable, ToTable: hostschema.FilesystemsTable,
			Cardinality: "oneToMany", Joins: hostJoins(),
		},
		{
			ID: "HostHasNetworkInterfaces", Name: "Host has network interfaces",
			Description: "A registered host reports its network interface inventory.",
			FromTable:   hostschema.HostTable, ToTable: hostschema.NetworkInterfacesTable,
			Cardinality: "oneToMany", Joins: hostJoins(),
		},
		{
			ID: "HostHasNetworkIO", Name: "Host has network I/O",
			Description: "A registered host reports cumulative network counters.",
			FromTable:   hostschema.HostTable, ToTable: hostschema.NetworkIOTable,
			Cardinality: "oneToMany", Joins: hostJoins(),
		},
		{
			ID: "HostHasProcesses", Name: "Host has processes",
			Description: "A registered host reports its current process inventory.",
			FromTable:   hostschema.HostTable, ToTable: hostschema.ProcessesTable,
			Cardinality: "oneToMany", Joins: hostJoins(),
		},
		{
			ID: "NetworkInterfaceHasIO", Name: "Network interface has I/O counters",
			Description: "A network interface is identified by the same host-local name as its counter row.",
			FromTable:   hostschema.NetworkInterfacesTable, ToTable: hostschema.NetworkIOTable,
			Cardinality: "oneToOne",
			Joins: []fieldJoin{
				{Left: "namespace", Right: "namespace"},
				{Left: "host_id", Right: "host_id"},
				{Left: "name", Right: "name"},
			},
		},
		{
			ID: "CPUInfoCorrespondsToCPUTimes", Name: "CPU information corresponds to CPU times",
			Description: "Logical processor identifiers correspond to counter names by an operating-system naming convention.",
			FromTable:   hostschema.CPUInfoTable, ToTable: hostschema.CPUTimesTable,
			Cardinality: "oneToOne",
		},
		{
			ID: "ProcessHasParentProcess", Name: "Process has parent process",
			Description: "A process reports its parent PID, but PID reuse prevents an exact stable join without the parent's creation time.",
			FromTable:   hostschema.ProcessesTable, ToTable: hostschema.ProcessesTable,
			Cardinality: "manyToOne",
		},
	}
}

func lowerCamel(value string) string {
	parts := strings.Split(value, "_")
	for index := range parts {
		if index == 0 {
			parts[index] = strings.ToLower(parts[index])
			continue
		}
		parts[index] = upperFirst(strings.ToLower(parts[index]))
	}
	return strings.Join(parts, "")
}

func displayName(value string) string {
	aliases := map[string]string{
		"cpu":    "CPU",
		"fifo":   "FIFO",
		"host":   "Host",
		"id":     "ID",
		"io":     "I/O",
		"iowait": "I/O wait",
		"irq":    "IRQ",
		"mhz":    "MHz",
		"mtu":    "MTU",
		"os":     "OS",
		"pid":    "PID",
	}
	parts := strings.Split(value, "_")
	for index, part := range parts {
		if alias, exists := aliases[part]; exists {
			parts[index] = alias
			continue
		}
		if index == 0 {
			parts[index] = upperFirst(part)
		} else {
			parts[index] = part
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
