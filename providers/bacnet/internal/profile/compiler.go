package profile

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"sort"
	"strings"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
)

var (
	profileIDPattern    = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	objectNamePattern   = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	tableNamePattern    = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)
	columnNamePattern   = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	propertyNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]*$`)
	namespacePattern    = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,126}$`)
)

type compiler struct {
	documents map[string]document
	states    map[string]uint8
	profiles  map[string]*Profile
}

type profileBuilder struct {
	objectTypes    map[string]ObjectType
	objectTypeByID map[uint32]string
	tables         map[string]*tableBuilder
}

type tableBuilder struct {
	name           string
	allObjectTypes bool
	objectTypes    map[string]struct{}
	columns        map[string]Column
	sources        []string
}

func compile(input []document) (*Catalog, error) {
	if len(input) == 0 {
		return nil, errors.New("at least one BACnet profile is required")
	}
	documents := make(map[string]document, len(input))
	for index, candidate := range input {
		normalized, err := normalizeDocument(candidate)
		if err != nil {
			return nil, fmt.Errorf("profile %d from %q: %w", index, candidate.source, err)
		}
		if previous, exists := documents[normalized.ID]; exists {
			return nil, fmt.Errorf(
				"duplicate BACnet profile %q in %q and %q",
				normalized.ID,
				previous.source,
				normalized.source,
			)
		}
		documents[normalized.ID] = normalized
	}
	standard, exists := documents[StandardProfileID]
	if !exists {
		return nil, fmt.Errorf("built-in profile %q is missing", StandardProfileID)
	}
	if standard.Abstract || len(standard.Selectors) != 0 || len(standard.Extends) != 0 {
		return nil, fmt.Errorf("built-in profile %q must be the concrete root profile", StandardProfileID)
	}

	implementation := &compiler{
		documents: documents,
		states:    make(map[string]uint8, len(documents)),
		profiles:  make(map[string]*Profile, len(documents)),
	}
	identifiers := sortedKeys(documents)
	for _, identifier := range identifiers {
		if identifier != StandardProfileID && !inheritsStandard(identifier, documents, nil) {
			return nil, fmt.Errorf(
				"profile %q must inherit %q",
				identifier,
				StandardProfileID,
			)
		}
		if _, err := implementation.compileProfile(identifier); err != nil {
			return nil, err
		}
	}
	ordered := make([]*Profile, 0, len(identifiers))
	for _, identifier := range identifiers {
		ordered = append(ordered, implementation.profiles[identifier])
	}
	if err := validateInheritancePrecedence(implementation.profiles); err != nil {
		return nil, err
	}
	if err := validateSelectorAmbiguity(ordered); err != nil {
		return nil, err
	}
	if err := validateCatalogCompatibility(ordered); err != nil {
		return nil, err
	}
	return &Catalog{
		profiles: implementation.profiles,
		ordered:  ordered,
		fallback: implementation.profiles[StandardProfileID],
	}, nil
}

func normalizeDocument(input document) (document, error) {
	input.APIVersion = strings.TrimSpace(input.APIVersion)
	input.ID = strings.TrimSpace(input.ID)
	if input.APIVersion != APIVersion {
		return document{}, fmt.Errorf("unsupported apiVersion %q", input.APIVersion)
	}
	if !profileIDPattern.MatchString(input.ID) {
		return document{}, fmt.Errorf("invalid profile id %q", input.ID)
	}
	if input.Abstract && len(input.Selectors) != 0 {
		return document{}, errors.New("abstract profile must not define selectors")
	}
	if input.ID != StandardProfileID && !input.Abstract && len(input.Selectors) == 0 {
		return document{}, errors.New("concrete profile must define at least one selector")
	}

	seenParents := make(map[string]struct{}, len(input.Extends))
	for index := range input.Extends {
		parent := strings.TrimSpace(input.Extends[index])
		if !profileIDPattern.MatchString(parent) {
			return document{}, fmt.Errorf("invalid parent profile id %q", input.Extends[index])
		}
		if parent == input.ID {
			return document{}, errors.New("profile must not extend itself")
		}
		if _, exists := seenParents[parent]; exists {
			return document{}, fmt.Errorf("duplicate parent profile %q", parent)
		}
		seenParents[parent] = struct{}{}
		input.Extends[index] = parent
	}
	slices.Sort(input.Extends)
	return input, nil
}

func inheritsStandard(identifier string, documents map[string]document, visiting map[string]bool) bool {
	if identifier == StandardProfileID {
		return true
	}
	if visiting == nil {
		visiting = make(map[string]bool)
	}
	if visiting[identifier] {
		return false
	}
	visiting[identifier] = true
	defer delete(visiting, identifier)
	document, exists := documents[identifier]
	if !exists {
		return false
	}
	for _, parent := range document.Extends {
		if inheritsStandard(parent, documents, visiting) {
			return true
		}
	}
	return false
}

func (c *compiler) compileProfile(identifier string) (*Profile, error) {
	switch c.states[identifier] {
	case 1:
		return nil, fmt.Errorf("BACnet profile inheritance cycle includes %q", identifier)
	case 2:
		return c.profiles[identifier], nil
	}
	document, exists := c.documents[identifier]
	if !exists {
		return nil, fmt.Errorf("BACnet profile %q does not exist", identifier)
	}
	c.states[identifier] = 1
	builder := newProfileBuilder()
	for _, parentID := range document.Extends {
		parent, err := c.compileProfile(parentID)
		if err != nil {
			return nil, fmt.Errorf("profile %q extends %q: %w", identifier, parentID, err)
		}
		if err := builder.mergeParent(parent); err != nil {
			return nil, fmt.Errorf("profile %q merges parent %q: %w", identifier, parentID, err)
		}
	}
	if err := builder.apply(document); err != nil {
		return nil, fmt.Errorf("compile profile %q: %w", identifier, err)
	}
	selectors, err := compileSelectors(document.Selectors)
	if err != nil {
		return nil, fmt.Errorf("compile profile %q selectors: %w", identifier, err)
	}
	profile, err := builder.freeze(document, selectors)
	if err != nil {
		return nil, fmt.Errorf("compile profile %q: %w", identifier, err)
	}
	c.profiles[identifier] = profile
	c.states[identifier] = 2
	return profile, nil
}

func newProfileBuilder() *profileBuilder {
	return &profileBuilder{
		objectTypes:    make(map[string]ObjectType),
		objectTypeByID: make(map[uint32]string),
		tables:         make(map[string]*tableBuilder),
	}
}

func (b *profileBuilder) mergeParent(parent *Profile) error {
	for _, objectType := range parent.ObjectTypes {
		if err := b.mergeObjectType(objectType); err != nil {
			return err
		}
	}
	for _, table := range parent.Tables {
		if err := b.mergeTable(table); err != nil {
			return err
		}
	}
	return nil
}

func (b *profileBuilder) apply(document document) error {
	seenObjectNames := make(map[string]struct{}, len(document.ObjectTypes))
	seenObjectIDs := make(map[uint32]struct{}, len(document.ObjectTypes))
	for index, input := range document.ObjectTypes {
		input.Name = strings.TrimSpace(input.Name)
		if !objectNamePattern.MatchString(input.Name) {
			return fmt.Errorf("objectTypes[%d] has invalid name %q", index, input.Name)
		}
		if input.ID > maximumObjectType {
			return fmt.Errorf("object type %q id %d exceeds %d", input.Name, input.ID, maximumObjectType)
		}
		if document.ID != StandardProfileID && input.ID < minimumProprietaryObjectType {
			return fmt.Errorf(
				"object type %q id %d is in the standard range; injected object types start at %d",
				input.Name,
				input.ID,
				minimumProprietaryObjectType,
			)
		}
		if _, exists := seenObjectNames[input.Name]; exists {
			return fmt.Errorf("duplicate object type name %q", input.Name)
		}
		if _, exists := seenObjectIDs[input.ID]; exists {
			return fmt.Errorf("duplicate object type id %d", input.ID)
		}
		seenObjectNames[input.Name] = struct{}{}
		seenObjectIDs[input.ID] = struct{}{}
		if err := b.mergeObjectType(ObjectType{
			ID:             input.ID,
			Name:           input.Name,
			SourceProfile:  document.ID,
			SourceProfiles: []string{document.ID},
		}); err != nil {
			return err
		}
	}

	seenTables := make(map[string]struct{}, len(document.Tables))
	for index, input := range document.Tables {
		input.Name = strings.TrimSpace(input.Name)
		if !tableNamePattern.MatchString(input.Name) {
			return fmt.Errorf("tables[%d] has invalid name %q", index, input.Name)
		}
		if _, exists := seenTables[input.Name]; exists {
			return fmt.Errorf("duplicate table %q", input.Name)
		}
		seenTables[input.Name] = struct{}{}
		if err := b.applyTable(document.ID, input); err != nil {
			return fmt.Errorf("table %q: %w", input.Name, err)
		}
	}
	return nil
}

func (b *profileBuilder) mergeObjectType(candidate ObjectType) error {
	if existing, exists := b.objectTypes[candidate.Name]; exists {
		if existing.ID != candidate.ID {
			return fmt.Errorf(
				"object type %q maps to ids %d and %d",
				candidate.Name,
				existing.ID,
				candidate.ID,
			)
		}
		existing.SourceProfiles = unionStrings(existing.SourceProfiles, candidate.SourceProfiles)
		b.objectTypes[candidate.Name] = existing
		return nil
	}
	if existingName, exists := b.objectTypeByID[candidate.ID]; exists {
		return fmt.Errorf(
			"object type id %d maps to names %q and %q",
			candidate.ID,
			existingName,
			candidate.Name,
		)
	}
	candidate.SourceProfiles = unionStrings(candidate.SourceProfiles)
	b.objectTypes[candidate.Name] = candidate
	b.objectTypeByID[candidate.ID] = candidate.Name
	return nil
}

func (b *profileBuilder) mergeTable(candidate Table) error {
	existing, exists := b.tables[candidate.Name]
	if !exists {
		existing = &tableBuilder{
			name:           candidate.Name,
			allObjectTypes: candidate.AllObjectTypes,
			objectTypes:    make(map[string]struct{}, len(candidate.ObjectTypes)),
			columns:        make(map[string]Column, len(candidate.Columns)),
			sources:        append([]string(nil), candidate.SourceProfiles...),
		}
		b.tables[candidate.Name] = existing
	} else if existing.allObjectTypes != candidate.AllObjectTypes {
		return fmt.Errorf("table %q has incompatible object type coverage", candidate.Name)
	}
	for _, objectType := range candidate.ObjectTypes {
		existing.objectTypes[objectType.Name] = struct{}{}
	}
	for _, column := range candidate.Columns {
		if err := mergeColumn(existing.columns, column, false); err != nil {
			return fmt.Errorf("table %q: %w", candidate.Name, err)
		}
	}
	existing.sources = unionStrings(existing.sources, candidate.SourceProfiles)
	return nil
}

func (b *profileBuilder) applyTable(profileID string, input tableDocument) error {
	if input.AllObjectTypes && len(input.ObjectTypes) != 0 {
		return errors.New("allObjectTypes and objectTypes are mutually exclusive")
	}
	table, exists := b.tables[input.Name]
	if !exists {
		if !input.AllObjectTypes && len(input.ObjectTypes) == 0 {
			return errors.New("new table requires allObjectTypes or objectTypes")
		}
		table = &tableBuilder{
			name:           input.Name,
			allObjectTypes: input.AllObjectTypes,
			objectTypes:    make(map[string]struct{}),
			columns:        make(map[string]Column),
		}
		b.tables[input.Name] = table
	} else if input.AllObjectTypes {
		table.allObjectTypes = true
		table.objectTypes = make(map[string]struct{})
	} else if table.allObjectTypes && len(input.ObjectTypes) != 0 {
		return errors.New("all-object table cannot be extended with explicit objectTypes")
	}
	for index, objectTypeName := range input.ObjectTypes {
		name := strings.TrimSpace(objectTypeName)
		if _, exists := b.objectTypes[name]; !exists {
			return fmt.Errorf("objectTypes[%d] references unknown object type %q", index, name)
		}
		table.objectTypes[name] = struct{}{}
	}
	table.sources = unionStrings(table.sources, []string{profileID})

	seenColumns := make(map[string]struct{}, len(input.Columns))
	for index, inputColumn := range input.Columns {
		inputColumn.Name = strings.TrimSpace(inputColumn.Name)
		if _, exists := seenColumns[inputColumn.Name]; exists {
			return fmt.Errorf("duplicate column %q", inputColumn.Name)
		}
		seenColumns[inputColumn.Name] = struct{}{}
		column, err := compileColumn(profileID, inputColumn, table, b.objectTypes)
		if err != nil {
			return fmt.Errorf("columns[%d] %q: %w", index, inputColumn.Name, err)
		}
		if err := mergeColumn(table.columns, column, inputColumn.Override); err != nil {
			return err
		}
	}
	return nil
}

func mergeColumn(columns map[string]Column, candidate Column, override bool) error {
	existing, exists := columns[candidate.Name]
	if !exists {
		if override {
			return fmt.Errorf("column %q declares override but no inherited column exists", candidate.Name)
		}
		columns[candidate.Name] = candidate
		return nil
	}
	if columnsEquivalent(existing, candidate) {
		existing.SourceProfiles = unionStrings(existing.SourceProfiles, candidate.SourceProfiles)
		columns[candidate.Name] = existing
		return nil
	}
	if !override {
		return fmt.Errorf(
			"column %q conflicts with definition from profile %q; set override: true to replace it",
			candidate.Name,
			existing.SourceProfile,
		)
	}
	candidate.SourceProfiles = unionStrings(existing.SourceProfiles, candidate.SourceProfiles)
	columns[candidate.Name] = candidate
	return nil
}

func compileColumn(
	profileID string,
	input columnDocument,
	table *tableBuilder,
	objectTypes map[string]ObjectType,
) (Column, error) {
	if !columnNamePattern.MatchString(input.Name) {
		return Column{}, fmt.Errorf("invalid logical column name %q", input.Name)
	}
	input.Property.Name = strings.TrimSpace(input.Property.Name)
	if !propertyNamePattern.MatchString(input.Property.Name) {
		return Column{}, fmt.Errorf("invalid BACnet property name %q", input.Property.Name)
	}
	if input.Property.ID > maximumPropertyID {
		return Column{}, fmt.Errorf(
			"BACnet property %q id %d exceeds %d",
			input.Property.Name,
			input.Property.ID,
			maximumPropertyID,
		)
	}
	input.BACnetType = BACnetType(strings.ToLower(strings.TrimSpace(string(input.BACnetType))))
	logicalType, err := parseLogicalType(input.KublingType)
	if err != nil {
		return Column{}, err
	}
	if !compatibleTypes(input.BACnetType, logicalType) {
		return Column{}, fmt.Errorf(
			"BACnet type %q is incompatible with Kubling type %s",
			input.BACnetType,
			logicalType,
		)
	}
	if len(input.Enum) != 0 && input.BACnetType != BACnetEnumerated {
		return Column{}, errors.New("enum is allowed only for BACnet enumerated properties")
	}
	enumeration := make(map[uint32]string, len(input.Enum))
	seenLabels := make(map[string]struct{}, len(input.Enum))
	for value, rawLabel := range input.Enum {
		label := strings.TrimSpace(rawLabel)
		if label == "" {
			return Column{}, fmt.Errorf("enum value %d has an empty label", value)
		}
		key := strings.ToLower(label)
		if _, exists := seenLabels[key]; exists {
			return Column{}, fmt.Errorf("enum label %q is duplicated", label)
		}
		seenLabels[key] = struct{}{}
		enumeration[value] = label
	}
	nullable := !input.Required
	if input.Nullable != nil {
		nullable = *input.Nullable
	}
	typeDescriptor := &kublingv1.TypeDescriptor{Type: logicalType}
	if input.Array {
		typeDescriptor = &kublingv1.TypeDescriptor{
			Type:        kublingv1.ValueType_VALUE_TYPE_ARRAY,
			ElementType: typeDescriptor,
		}
	}
	write, err := compileWritePolicy(input.Write, input.Array, input.BACnetType, table, objectTypes)
	if err != nil {
		return Column{}, err
	}
	return Column{
		Name: input.Name,
		Property: Property{
			ID:   input.Property.ID,
			Name: input.Property.Name,
		},
		BACnetType:     input.BACnetType,
		Type:           typeDescriptor,
		Nullable:       nullable,
		Required:       input.Required,
		Enum:           enumeration,
		Write:          write,
		SourceProfile:  profileID,
		SourceProfiles: []string{profileID},
	}, nil
}

func compileWritePolicy(
	input *writeDocument,
	array bool,
	nativeType BACnetType,
	table *tableBuilder,
	objectTypes map[string]ObjectType,
) (*WritePolicy, error) {
	if input == nil {
		return nil, nil
	}
	if array {
		return nil, errors.New("array properties are not writable in this provider version")
	}
	switch nativeType {
	case BACnetBoolean,
		BACnetUnsigned,
		BACnetSigned,
		BACnetReal,
		BACnetDouble,
		BACnetOctetString,
		BACnetCharacterString,
		BACnetEnumerated:
	default:
		return nil, fmt.Errorf("BACnet type %q is not writable in this provider version", nativeType)
	}
	if len(input.ObjectTypes) == 0 {
		return nil, errors.New("write.objectTypes must contain at least one object type")
	}
	result := &WritePolicy{Commandable: input.Commandable}
	seen := make(map[string]struct{}, len(input.ObjectTypes))
	for index, raw := range input.ObjectTypes {
		name := strings.TrimSpace(raw)
		if _, exists := objectTypes[name]; !exists {
			return nil, fmt.Errorf("write.objectTypes[%d] references unknown object type %q", index, name)
		}
		if !table.allObjectTypes {
			if _, exists := table.objectTypes[name]; !exists {
				return nil, fmt.Errorf(
					"write.objectTypes[%d] references object type %q outside this table",
					index,
					name,
				)
			}
		}
		if _, exists := seen[name]; exists {
			return nil, fmt.Errorf("write.objectTypes[%d] duplicates %q", index, name)
		}
		seen[name] = struct{}{}
		result.ObjectTypes = append(result.ObjectTypes, name)
	}
	slices.Sort(result.ObjectTypes)
	return result, nil
}

func parseLogicalType(raw string) (kublingv1.ValueType, error) {
	name := strings.ToUpper(strings.TrimSpace(raw))
	valueTypes := map[string]kublingv1.ValueType{
		"STRING":     kublingv1.ValueType_VALUE_TYPE_STRING,
		"VARBINARY":  kublingv1.ValueType_VALUE_TYPE_VARBINARY,
		"BOOLEAN":    kublingv1.ValueType_VALUE_TYPE_BOOLEAN,
		"BYTE":       kublingv1.ValueType_VALUE_TYPE_BYTE,
		"SHORT":      kublingv1.ValueType_VALUE_TYPE_SHORT,
		"INTEGER":    kublingv1.ValueType_VALUE_TYPE_INTEGER,
		"LONG":       kublingv1.ValueType_VALUE_TYPE_LONG,
		"BIGINTEGER": kublingv1.ValueType_VALUE_TYPE_BIGINTEGER,
		"FLOAT":      kublingv1.ValueType_VALUE_TYPE_FLOAT,
		"DOUBLE":     kublingv1.ValueType_VALUE_TYPE_DOUBLE,
		"BIGDECIMAL": kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL,
		"DATE":       kublingv1.ValueType_VALUE_TYPE_DATE,
		"TIME":       kublingv1.ValueType_VALUE_TYPE_TIME,
		"TIMESTAMP":  kublingv1.ValueType_VALUE_TYPE_TIMESTAMP,
		"JSON":       kublingv1.ValueType_VALUE_TYPE_JSON,
	}
	logicalType, exists := valueTypes[name]
	if !exists {
		return 0, fmt.Errorf("unsupported Kubling type %q", raw)
	}
	return logicalType, nil
}

func compatibleTypes(native BACnetType, logical kublingv1.ValueType) bool {
	switch native {
	case BACnetBoolean:
		return logical == kublingv1.ValueType_VALUE_TYPE_BOOLEAN
	case BACnetUnsigned:
		return logical == kublingv1.ValueType_VALUE_TYPE_INTEGER ||
			logical == kublingv1.ValueType_VALUE_TYPE_LONG ||
			logical == kublingv1.ValueType_VALUE_TYPE_BIGINTEGER
	case BACnetSigned:
		return logical == kublingv1.ValueType_VALUE_TYPE_INTEGER ||
			logical == kublingv1.ValueType_VALUE_TYPE_LONG ||
			logical == kublingv1.ValueType_VALUE_TYPE_BIGINTEGER
	case BACnetReal:
		return logical == kublingv1.ValueType_VALUE_TYPE_FLOAT ||
			logical == kublingv1.ValueType_VALUE_TYPE_DOUBLE ||
			logical == kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL
	case BACnetDouble:
		return logical == kublingv1.ValueType_VALUE_TYPE_DOUBLE ||
			logical == kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL
	case BACnetOctetString:
		return logical == kublingv1.ValueType_VALUE_TYPE_VARBINARY
	case BACnetCharacterString:
		return logical == kublingv1.ValueType_VALUE_TYPE_STRING
	case BACnetBitString:
		return logical == kublingv1.ValueType_VALUE_TYPE_VARBINARY ||
			logical == kublingv1.ValueType_VALUE_TYPE_STRING
	case BACnetEnumerated:
		return logical == kublingv1.ValueType_VALUE_TYPE_STRING ||
			logical == kublingv1.ValueType_VALUE_TYPE_INTEGER ||
			logical == kublingv1.ValueType_VALUE_TYPE_LONG
	case BACnetDate:
		return logical == kublingv1.ValueType_VALUE_TYPE_DATE ||
			logical == kublingv1.ValueType_VALUE_TYPE_STRING
	case BACnetTime:
		return logical == kublingv1.ValueType_VALUE_TYPE_TIME ||
			logical == kublingv1.ValueType_VALUE_TYPE_STRING
	case BACnetObjectIdentifier:
		return logical == kublingv1.ValueType_VALUE_TYPE_STRING
	default:
		return false
	}
}

func (b *profileBuilder) freeze(document document, selectors []Selector) (*Profile, error) {
	objectTypes := make([]ObjectType, 0, len(b.objectTypes))
	for _, objectType := range b.objectTypes {
		objectType.SourceProfiles = unionStrings(objectType.SourceProfiles)
		objectTypes = append(objectTypes, objectType)
	}
	slices.SortFunc(objectTypes, func(left ObjectType, right ObjectType) int {
		if left.ID != right.ID {
			if left.ID < right.ID {
				return -1
			}
			return 1
		}
		return strings.Compare(left.Name, right.Name)
	})

	tables := make([]Table, 0, len(b.tables))
	for _, input := range b.tables {
		if len(input.columns) == 0 {
			return nil, fmt.Errorf("table %q has no columns", input.name)
		}
		if !input.allObjectTypes && len(input.objectTypes) == 0 {
			return nil, fmt.Errorf("table %q has no object types", input.name)
		}
		resolvedObjectTypes := make([]ObjectType, 0, len(input.objectTypes))
		for name := range input.objectTypes {
			resolvedObjectTypes = append(resolvedObjectTypes, b.objectTypes[name])
		}
		slices.SortFunc(resolvedObjectTypes, func(left ObjectType, right ObjectType) int {
			if left.ID < right.ID {
				return -1
			}
			if left.ID > right.ID {
				return 1
			}
			return strings.Compare(left.Name, right.Name)
		})
		columns := make([]Column, 0, len(input.columns))
		for _, column := range input.columns {
			column.SourceProfiles = unionStrings(column.SourceProfiles)
			columns = append(columns, column)
		}
		slices.SortFunc(columns, func(left Column, right Column) int {
			return strings.Compare(left.Name, right.Name)
		})
		tables = append(tables, Table{
			Name:           input.name,
			AllObjectTypes: input.allObjectTypes,
			ObjectTypes:    resolvedObjectTypes,
			Columns:        columns,
			SourceProfiles: unionStrings(input.sources),
		})
	}
	slices.SortFunc(tables, func(left Table, right Table) int {
		return strings.Compare(left.Name, right.Name)
	})
	return &Profile{
		ID:          document.ID,
		Abstract:    document.Abstract,
		Extends:     append([]string(nil), document.Extends...),
		Selectors:   selectors,
		ObjectTypes: objectTypes,
		Tables:      tables,
	}, nil
}

func compileSelectors(input []selectorDocument) ([]Selector, error) {
	result := make([]Selector, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for index, candidate := range input {
		if candidate.VendorIdentifier != nil && *candidate.VendorIdentifier > maximumVendorIdentifier {
			return nil, fmt.Errorf(
				"selectors[%d] vendorIdentifier %d exceeds %d",
				index,
				*candidate.VendorIdentifier,
				maximumVendorIdentifier,
			)
		}
		if candidate.DeviceInstance != nil && *candidate.DeviceInstance > maximumDeviceInstance {
			return nil, fmt.Errorf(
				"selectors[%d] deviceInstance %d exceeds %d",
				index,
				*candidate.DeviceInstance,
				maximumDeviceInstance,
			)
		}
		selector := Selector{
			VendorIdentifier: cloneUint32(candidate.VendorIdentifier),
			ModelName:        strings.TrimSpace(candidate.ModelName),
			FirmwareRevision: strings.TrimSpace(candidate.FirmwareRevision),
			DeviceInstance:   cloneUint32(candidate.DeviceInstance),
			Namespace:        strings.TrimSpace(candidate.Namespace),
			ScopeID:          strings.TrimSpace(candidate.ScopeID),
		}
		if selector.rank() == 0 {
			return nil, fmt.Errorf("selectors[%d] contains no constraints", index)
		}
		if selector.ModelName != "" && selector.VendorIdentifier == nil {
			return nil, fmt.Errorf(
				"selectors[%d] modelName requires vendorIdentifier",
				index,
			)
		}
		if selector.FirmwareRevision != "" && selector.ModelName == "" {
			return nil, fmt.Errorf(
				"selectors[%d] firmwareRevision requires modelName",
				index,
			)
		}
		if selector.DeviceInstance == nil && selector.VendorIdentifier == nil {
			return nil, fmt.Errorf(
				"selectors[%d] requires vendorIdentifier or deviceInstance",
				index,
			)
		}
		if selector.DeviceInstance == nil && (selector.Namespace != "" || selector.ScopeID != "") {
			return nil, fmt.Errorf(
				"selectors[%d] namespace and scopeId are allowed only for a device override",
				index,
			)
		}
		if selector.DeviceInstance != nil {
			if !namespacePattern.MatchString(selector.Namespace) {
				return nil, fmt.Errorf(
					"selectors[%d] device override requires a valid namespace",
					index,
				)
			}
			if !profileIDPattern.MatchString(selector.ScopeID) {
				return nil, fmt.Errorf(
					"selectors[%d] device override requires a valid scopeId",
					index,
				)
			}
		}
		key := selectorKey(selector)
		if _, exists := seen[key]; exists {
			return nil, fmt.Errorf("selectors[%d] duplicates another selector", index)
		}
		seen[key] = struct{}{}
		result = append(result, selector)
	}
	slices.SortFunc(result, func(left Selector, right Selector) int {
		return strings.Compare(selectorKey(left), selectorKey(right))
	})
	return result, nil
}

func selectorKey(selector Selector) string {
	vendor := "*"
	if selector.VendorIdentifier != nil {
		vendor = fmt.Sprint(*selector.VendorIdentifier)
	}
	device := "*"
	if selector.DeviceInstance != nil {
		device = fmt.Sprint(*selector.DeviceInstance)
	}
	return fmt.Sprintf(
		"%02d|%s|%s|%s|%s|%s|%s",
		selector.rank(),
		vendor,
		selector.ModelName,
		selector.FirmwareRevision,
		device,
		selector.Namespace,
		selector.ScopeID,
	)
}

func columnsEquivalent(left Column, right Column) bool {
	return left.Name == right.Name &&
		left.Property == right.Property &&
		left.BACnetType == right.BACnetType &&
		typeDescriptorsEqual(left.Type, right.Type) &&
		left.Nullable == right.Nullable &&
		left.Required == right.Required &&
		maps.Equal(left.Enum, right.Enum) &&
		writePoliciesEqual(left.Write, right.Write)
}

func writePoliciesEqual(left *WritePolicy, right *WritePolicy) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.Commandable == right.Commandable &&
		slices.Equal(left.ObjectTypes, right.ObjectTypes)
}

func typeDescriptorsEqual(left *kublingv1.TypeDescriptor, right *kublingv1.TypeDescriptor) bool {
	if left == nil || right == nil {
		return left == right
	}
	return left.GetType() == right.GetType() &&
		typeDescriptorsEqual(left.GetElementType(), right.GetElementType())
}

func validateCatalogCompatibility(profiles []*Profile) error {
	type declaredColumn struct {
		profile string
		column  Column
	}
	columns := make(map[string]declaredColumn)
	for _, candidate := range profiles {
		if candidate.Abstract {
			continue
		}
		for _, table := range candidate.Tables {
			for _, column := range table.Columns {
				key := table.Name + "." + column.Name
				existing, exists := columns[key]
				if !exists {
					columns[key] = declaredColumn{profile: candidate.ID, column: column}
					continue
				}
				if !typeDescriptorsEqual(existing.column.Type, column.Type) {
					return fmt.Errorf(
						"profiles %q and %q expose incompatible logical types for %s",
						existing.profile,
						candidate.ID,
						key,
					)
				}
			}
		}
	}
	return nil
}

func cloneUint32(value *uint32) *uint32 {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func unionStrings(inputs ...[]string) []string {
	unique := make(map[string]struct{})
	for _, input := range inputs {
		for _, value := range input {
			unique[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(unique))
	for value := range unique {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func sortedKeys[V any](input map[string]V) []string {
	result := make([]string, 0, len(input))
	for key := range input {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}
