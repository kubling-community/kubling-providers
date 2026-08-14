package kubernetes

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type resourceDescriptor struct {
	groupVersion schema.GroupVersion
	resource     metav1.APIResource
	baseName     string
	tableName    string
}

func buildMetadata(
	resourceLists []*metav1.APIResourceList,
	failedGroupVersions []string,
) *providerv1.SchemaMetadata {
	return buildMetadataWithSchema(
		resourceLists,
		failedGroupVersions,
		SchemaConfig{},
	)
}

func buildMetadataWithSchema(
	resourceLists []*metav1.APIResourceList,
	failedGroupVersions []string,
	schemaConfig SchemaConfig,
	resolvers ...*openAPISchemaResolver,
) *providerv1.SchemaMetadata {
	descriptors := discoverableResources(resourceLists)
	descriptors = filterResourceDescriptors(descriptors, schemaConfig)
	assignTableNames(descriptors)

	var resolver *openAPISchemaResolver
	if len(resolvers) > 0 {
		resolver = resolvers[0]
	}

	failedGroupVersions = append([]string(nil), failedGroupVersions...)
	sort.Strings(failedGroupVersions)

	metadata := &providerv1.SchemaMetadata{
		Properties: map[string]string{
			"kubernetes.resource_count": strconv.Itoa(len(descriptors)),
		},
	}
	if len(failedGroupVersions) > 0 {
		metadata.Properties["kubernetes.discovery_partial"] = "true"
		metadata.Properties["kubernetes.failed_group_versions"] = strings.Join(failedGroupVersions, ",")
	}

	groups := make(map[string]schema.GroupVersion)
	for _, descriptor := range descriptors {
		groups[descriptor.groupVersion.String()] = descriptor.groupVersion
		depth := schemaConfig.expansionDepth(
			descriptor.groupVersion.String(),
			descriptor.resource.Name,
		)
		metadata.Tables = append(
			metadata.Tables,
			resourceTableMetadataWithSchema(
				descriptor,
				resolver,
				depth,
				schemaConfig.includeObject(),
			),
		)
	}
	sort.Slice(metadata.Tables, func(left int, right int) bool {
		return metadata.Tables[left].GetName() < metadata.Tables[right].GetName()
	})

	groupVersions := make([]string, 0, len(groups))
	for groupVersion := range groups {
		groupVersions = append(groupVersions, groupVersion)
	}
	sort.Strings(groupVersions)
	metadata.Properties["kubernetes.group_version_count"] = strconv.Itoa(len(groupVersions))
	for _, groupVersion := range groupVersions {
		parsed := groups[groupVersion]
		metadata.Namespaces = append(metadata.Namespaces, &providerv1.NamespaceMetadata{
			Name:       groupVersion,
			Annotation: fmt.Sprintf("Kubernetes API group version %s", groupVersion),
			Properties: map[string]string{
				"kubernetes.group":   parsed.Group,
				"kubernetes.version": parsed.Version,
			},
		})
	}

	return metadata
}

func filterResourceDescriptors(
	descriptors []*resourceDescriptor,
	schemaConfig SchemaConfig,
) []*resourceDescriptor {
	if len(descriptors) == 0 {
		return descriptors
	}
	filtered := make([]*resourceDescriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		if descriptor == nil {
			continue
		}
		if !schemaConfig.includesResource(
			descriptor.groupVersion.String(),
			descriptor.resource.Name,
		) {
			continue
		}
		filtered = append(filtered, descriptor)
	}
	return filtered
}

func applyNamespaceInsertDefaults(
	metadata *providerv1.SchemaMetadata,
	strategy BlankNamespaceStrategy,
	defaultNamespace string,
) {
	if metadata == nil || strategy != BlankNamespaceDefault || strings.TrimSpace(defaultNamespace) == "" {
		return
	}
	for _, table := range metadata.GetTables() {
		if table.GetProperties()["kubernetes.namespaced"] != "true" {
			continue
		}
		column := columnByName(table.GetColumns(), "metadata__namespace")
		if column != nil && column.GetUpdatable() {
			column.DefaultExpression = sqlStringLiteralDefault(defaultNamespace)
		}
	}
}

func discoverableResources(resourceLists []*metav1.APIResourceList) []*resourceDescriptor {
	descriptors := make([]*resourceDescriptor, 0)
	for _, resourceList := range resourceLists {
		if resourceList == nil {
			continue
		}
		groupVersion, err := schema.ParseGroupVersion(resourceList.GroupVersion)
		if err != nil || groupVersion.Version == "" {
			continue
		}
		for _, resource := range resourceList.APIResources {
			if resource.Name == "" || resource.Kind == "" || strings.Contains(resource.Name, "/") || !hasVerb(resource.Verbs, "list") {
				continue
			}
			descriptors = append(descriptors, &resourceDescriptor{
				groupVersion: groupVersion,
				resource:     resource,
				baseName:     logicalIdentifier(resource.Kind),
			})
		}
	}
	sort.Slice(descriptors, func(left int, right int) bool {
		leftKey := descriptors[left].groupVersion.String() + "\x00" + descriptors[left].resource.Name
		rightKey := descriptors[right].groupVersion.String() + "\x00" + descriptors[right].resource.Name
		return leftKey < rightKey
	})
	return descriptors
}

func assignTableNames(descriptors []*resourceDescriptor) {
	baseCounts := make(map[string]int)
	for _, descriptor := range descriptors {
		baseCounts[descriptor.baseName]++
	}

	used := make(map[string]struct{}, len(descriptors))
	for _, descriptor := range descriptors {
		candidate := descriptor.baseName
		if baseCounts[descriptor.baseName] > 1 {
			group := logicalIdentifier(descriptor.groupVersion.Group)
			if group == "" {
				group = "CORE"
			}
			candidate = group + "_" + descriptor.baseName
		}
		if _, exists := used[candidate]; exists {
			candidate += "_" + logicalIdentifier(descriptor.resource.Name)
		}
		for suffix := 2; ; suffix++ {
			if _, exists := used[candidate]; !exists {
				break
			}
			candidate = fmt.Sprintf("%s_%d", candidate, suffix)
		}
		descriptor.tableName = candidate
		used[candidate] = struct{}{}
	}
}

func resourceTableMetadata(descriptor *resourceDescriptor) *providerv1.TableMetadata {
	return resourceTableMetadataWithOptions(descriptor, true)
}

func resourceTableMetadataWithOptions(
	descriptor *resourceDescriptor,
	includeObject bool,
) *providerv1.TableMetadata {
	insertable := hasVerb(descriptor.resource.Verbs, "create")
	mutable := hasVerb(descriptor.resource.Verbs, "update") || hasVerb(descriptor.resource.Verbs, "patch")
	deletable := hasVerb(descriptor.resource.Verbs, "delete")
	updatable := insertable || mutable || deletable
	namespace := descriptor.groupVersion.String()
	verbs := append([]string(nil), descriptor.resource.Verbs...)
	sort.Strings(verbs)
	properties := map[string]string{
		"kubernetes.group":      descriptor.groupVersion.Group,
		"kubernetes.version":    descriptor.groupVersion.Version,
		"kubernetes.resource":   descriptor.resource.Name,
		"kubernetes.kind":       descriptor.resource.Kind,
		"kubernetes.namespaced": strconv.FormatBool(descriptor.resource.Namespaced),
		"kubernetes.verbs":      strings.Join(verbs, ","),
	}
	if descriptor.resource.StorageVersionHash != "" {
		properties["kubernetes.storage_version_hash"] = descriptor.resource.StorageVersionHash
	}
	if len(descriptor.resource.ShortNames) > 0 {
		properties["kubernetes.short_names"] = strings.Join(descriptor.resource.ShortNames, ",")
	}
	if len(descriptor.resource.Categories) > 0 {
		properties["kubernetes.categories"] = strings.Join(descriptor.resource.Categories, ",")
	}

	columns := resourceColumnsWithOptions(descriptor, insertable, mutable, includeObject)
	identityColumns := []string{"metadata__name"}
	if descriptor.resource.Namespaced {
		identityColumns = []string{"metadata__namespace", "metadata__name"}
	}

	table := &providerv1.TableMetadata{
		Name:       descriptor.tableName,
		SourceName: descriptor.resource.Name,
		Namespace:  namespace,
		Kind:       providerv1.TableKind_TABLE_KIND_TABLE,
		Updatable:  &updatable,
		Annotation: fmt.Sprintf("Kubernetes %s resource from %s", descriptor.resource.Kind, namespace),
		Properties: properties,
		Columns:    columns,
		Keys: []*providerv1.KeyMetadata{
			{
				Name:    "UK_" + descriptor.tableName + "_RESOURCE_IDENTITY",
				Kind:    providerv1.KeyKind_KEY_KIND_UNIQUE,
				Columns: identityColumns,
			},
			{
				Name:    "UK_" + descriptor.tableName + "_UID",
				Kind:    providerv1.KeyKind_KEY_KIND_UNIQUE,
				Columns: []string{"metadata__uid"},
			},
		},
	}
	providersdk.MustAddStablePrimaryKey(table, "identifier", identityColumns...)
	return table
}

func resourceColumns(descriptor *resourceDescriptor, insertable bool, mutable bool) []*providerv1.ColumnMetadata {
	return resourceColumnsWithOptions(descriptor, insertable, mutable, true)
}

func resourceColumnsWithOptions(
	descriptor *resourceDescriptor,
	insertable bool,
	mutable bool,
	includeObject bool,
) []*providerv1.ColumnMetadata {
	notNullable := false
	nullable := true
	notUpdatable := false
	insertInput := insertable
	documentInput := insertable || mutable
	equality := providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_EQUALITY
	unsearchable := providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE
	columns := []*providerv1.ColumnMetadata{
		stringColumn("api_version", "apiVersion", &notNullable, &insertInput, unsearchable),
		stringColumn("kind", "kind", &notNullable, &insertInput, unsearchable),
		stringColumn("metadata__uid", "metadata.uid", &notNullable, &notUpdatable, unsearchable),
		stringColumn("metadata__name", "metadata.name", &notNullable, &insertInput, equality),
	}
	if descriptor.resource.Namespaced {
		columns = append(columns, stringColumn("metadata__namespace", "metadata.namespace", &notNullable, &insertInput, equality))
	} else {
		columns = append(columns, stringColumn("metadata__namespace", "metadata.namespace", &nullable, &insertInput, equality))
	}
	columns = append(columns,
		stringColumn("metadata__resource_version", "metadata.resourceVersion", &notNullable, &notUpdatable, unsearchable),
		jsonColumn("metadata", "metadata", &notNullable, &documentInput),
		jsonColumn("spec", "spec", &nullable, &documentInput),
		jsonColumn("status", "status", &nullable, &notUpdatable),
	)
	if includeObject {
		columns = append(columns, jsonColumn("object", "$", &notNullable, &documentInput))
	}
	if insertable {
		columns[0].DefaultExpression = sqlStringLiteralDefault(descriptor.groupVersion.String())
		columns[1].DefaultExpression = sqlStringLiteralDefault(descriptor.resource.Kind)
		columnByName(columns, "metadata").DefaultExpression = "jsonParse('{}', true)"
		if object := columnByName(columns, "object"); object != nil {
			object.DefaultExpression = "jsonParse('{}', true)"
		}
	}
	return columns
}

func columnByName(columns []*providerv1.ColumnMetadata, name string) *providerv1.ColumnMetadata {
	for _, column := range columns {
		if strings.EqualFold(column.GetName(), name) {
			return column
		}
	}
	return nil
}

func sqlStringLiteralDefault(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func stringColumn(
	name string,
	sourceName string,
	nullable *bool,
	updatable *bool,
	searchability providerv1.ColumnSearchability,
) *providerv1.ColumnMetadata {
	return &providerv1.ColumnMetadata{
		Name:          name,
		SourceName:    sourceName,
		Type:          kublingv1.ValueType_VALUE_TYPE_STRING,
		NativeType:    "string",
		Nullable:      nullable,
		Updatable:     updatable,
		Searchability: searchability,
	}
}

func jsonColumn(
	name string,
	sourceName string,
	nullable *bool,
	updatable *bool,
) *providerv1.ColumnMetadata {
	return &providerv1.ColumnMetadata{
		Name:          name,
		SourceName:    sourceName,
		Type:          kublingv1.ValueType_VALUE_TYPE_JSON,
		NativeType:    "object",
		Nullable:      nullable,
		Updatable:     updatable,
		Searchability: providerv1.ColumnSearchability_COLUMN_SEARCHABILITY_UNSEARCHABLE,
	}
}

func hasVerb(verbs metav1.Verbs, expected string) bool {
	for _, verb := range verbs {
		if verb == expected {
			return true
		}
	}
	return false
}

func logicalIdentifier(value string) string {
	runes := []rune(strings.TrimSpace(value))
	var builder strings.Builder
	underscore := false
	for index, current := range runes {
		if !unicode.IsLetter(current) && !unicode.IsDigit(current) {
			underscore = builder.Len() > 0
			continue
		}
		if underscore {
			builder.WriteByte('_')
			underscore = false
		}
		if unicode.IsUpper(current) && builder.Len() > 0 {
			previous := runes[index-1]
			var next rune
			if index+1 < len(runes) {
				next = runes[index+1]
			}
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || unicode.IsLower(next) && unicode.IsUpper(previous) {
				if !strings.HasSuffix(builder.String(), "_") {
					builder.WriteByte('_')
				}
			}
		}
		builder.WriteRune(unicode.ToUpper(current))
	}
	return strings.Trim(builder.String(), "_")
}
