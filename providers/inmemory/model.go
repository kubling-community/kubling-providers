package inmemory

import (
	"fmt"
	"strings"
	"sync"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	projectEntityName    = "PROJECT"
	taskEntityName       = "TASK"
	auditEventEntityName = "AUDIT_EVENT"
	typeSampleEntityName = "TYPE_SAMPLE"
)

type task struct {
	ID            string
	ProjectID     string
	Title         string
	Description   *string
	Completed     bool
	Priority      int32
	EstimateHours *float32
	DueAt         *string
}

type entityRow struct {
	key    string
	values map[string]*kublingv1.Value
}

type store struct {
	mu       sync.RWMutex
	tasks    map[string]task
	entities map[string][]entityRow
	nextID   uint64
}

type entityField struct {
	name      string
	valueType kublingv1.ValueType
}

type entityDefinition struct {
	name    string
	fields  []entityField
	mutable bool
}

var (
	projectFields = []entityField{
		{name: "id", valueType: kublingv1.ValueType_VALUE_TYPE_STRING},
		{name: "name", valueType: kublingv1.ValueType_VALUE_TYPE_STRING},
		{name: "status", valueType: kublingv1.ValueType_VALUE_TYPE_CHAR},
		{name: "active", valueType: kublingv1.ValueType_VALUE_TYPE_BOOLEAN},
		{name: "budget", valueType: kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL},
		{name: "started_on", valueType: kublingv1.ValueType_VALUE_TYPE_DATE},
		{name: "metadata", valueType: kublingv1.ValueType_VALUE_TYPE_JSON},
	}

	taskFields = []entityField{
		{name: "id", valueType: kublingv1.ValueType_VALUE_TYPE_STRING},
		{name: "project_id", valueType: kublingv1.ValueType_VALUE_TYPE_STRING},
		{name: "title", valueType: kublingv1.ValueType_VALUE_TYPE_STRING},
		{name: "description", valueType: kublingv1.ValueType_VALUE_TYPE_CLOB},
		{name: "completed", valueType: kublingv1.ValueType_VALUE_TYPE_BOOLEAN},
		{name: "priority", valueType: kublingv1.ValueType_VALUE_TYPE_INTEGER},
		{name: "estimate_hours", valueType: kublingv1.ValueType_VALUE_TYPE_FLOAT},
		{name: "due_at", valueType: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP},
	}

	auditEventFields = []entityField{
		{name: "id", valueType: kublingv1.ValueType_VALUE_TYPE_LONG},
		{name: "entity_type", valueType: kublingv1.ValueType_VALUE_TYPE_STRING},
		{name: "entity_id", valueType: kublingv1.ValueType_VALUE_TYPE_STRING},
		{name: "action", valueType: kublingv1.ValueType_VALUE_TYPE_CHAR},
		{name: "sequence", valueType: kublingv1.ValueType_VALUE_TYPE_SHORT},
		{name: "risk_score", valueType: kublingv1.ValueType_VALUE_TYPE_DOUBLE},
		{name: "occurred_at", valueType: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP},
		{name: "source_address", valueType: kublingv1.ValueType_VALUE_TYPE_VARBINARY},
		{name: "payload", valueType: kublingv1.ValueType_VALUE_TYPE_JSON},
		{name: "raw_payload", valueType: kublingv1.ValueType_VALUE_TYPE_BLOB},
	}

	typeSampleFields = []entityField{
		{name: "sample_id", valueType: kublingv1.ValueType_VALUE_TYPE_STRING},
		{name: "string_value", valueType: kublingv1.ValueType_VALUE_TYPE_STRING},
		{name: "varbinary_value", valueType: kublingv1.ValueType_VALUE_TYPE_VARBINARY},
		{name: "char_value", valueType: kublingv1.ValueType_VALUE_TYPE_CHAR},
		{name: "boolean_value", valueType: kublingv1.ValueType_VALUE_TYPE_BOOLEAN},
		{name: "byte_value", valueType: kublingv1.ValueType_VALUE_TYPE_BYTE},
		{name: "short_value", valueType: kublingv1.ValueType_VALUE_TYPE_SHORT},
		{name: "integer_value", valueType: kublingv1.ValueType_VALUE_TYPE_INTEGER},
		{name: "long_value", valueType: kublingv1.ValueType_VALUE_TYPE_LONG},
		{name: "biginteger_value", valueType: kublingv1.ValueType_VALUE_TYPE_BIGINTEGER},
		{name: "float_value", valueType: kublingv1.ValueType_VALUE_TYPE_FLOAT},
		{name: "double_value", valueType: kublingv1.ValueType_VALUE_TYPE_DOUBLE},
		{name: "bigdecimal_value", valueType: kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL},
		{name: "date_value", valueType: kublingv1.ValueType_VALUE_TYPE_DATE},
		{name: "time_value", valueType: kublingv1.ValueType_VALUE_TYPE_TIME},
		{name: "timestamp_value", valueType: kublingv1.ValueType_VALUE_TYPE_TIMESTAMP},
		{name: "blob_value", valueType: kublingv1.ValueType_VALUE_TYPE_BLOB},
		{name: "clob_value", valueType: kublingv1.ValueType_VALUE_TYPE_CLOB},
		{name: "geometry_value", valueType: kublingv1.ValueType_VALUE_TYPE_GEOMETRY},
		{name: "geography_value", valueType: kublingv1.ValueType_VALUE_TYPE_GEOGRAPHY},
		{name: "json_value", valueType: kublingv1.ValueType_VALUE_TYPE_JSON},
		{name: "xml_value", valueType: kublingv1.ValueType_VALUE_TYPE_XML},
	}

	entityDefinitions = []*entityDefinition{
		{name: projectEntityName, fields: projectFields},
		{name: taskEntityName, fields: taskFields, mutable: true},
		{name: auditEventEntityName, fields: auditEventFields},
		{name: typeSampleEntityName, fields: typeSampleFields},
	}
)

func newStore() *store {
	description := "Validate the public provider lifecycle through the SDK adapter."
	estimateTwo := float32(2.5)
	estimateFour := float32(4)
	dueTomorrow := "2026-08-04T17:00:00"
	dueFriday := "2026-08-07T12:30:00"

	return &store{
		tasks: map[string]task{
			"task-1": {
				ID:            "task-1",
				ProjectID:     "project-1",
				Title:         "Review provider SDK",
				Description:   &description,
				Completed:     true,
				Priority:      1,
				EstimateHours: &estimateTwo,
				DueAt:         &dueTomorrow,
			},
			"task-2": {
				ID:            "task-2",
				ProjectID:     "project-1",
				Title:         "Build in-memory provider",
				Priority:      2,
				EstimateHours: &estimateFour,
				DueAt:         &dueFriday,
			},
			"task-3": {
				ID:        "task-3",
				ProjectID: "project-2",
				Title:     "Connect Kubling",
				Priority:  3,
			},
		},
		entities: map[string][]entityRow{
			projectEntityName:    projectRows(),
			auditEventEntityName: auditEventRows(),
			typeSampleEntityName: typeSampleRows(),
		},
		nextID: 4,
	}
}

func projectRows() []entityRow {
	return []entityRow{
		newEntityRow("project-1", map[string]*kublingv1.Value{
			"id":         stringValue("project-1"),
			"name":       stringValue("Provider SDK"),
			"status":     charValue("A"),
			"active":     booleanValue(true),
			"budget":     bigdecimalValue("125000.50"),
			"started_on": dateValue("2026-07-01"),
			"metadata":   jsonValue(`{"owner":"platform","region":"eu"}`),
		}),
		newEntityRow("project-2", map[string]*kublingv1.Value{
			"id":         stringValue("project-2"),
			"name":       stringValue("Engine Integration"),
			"status":     charValue("P"),
			"active":     booleanValue(true),
			"budget":     bigdecimalValue("87500.00"),
			"started_on": dateValue("2026-08-01"),
			"metadata":   jsonValue(`{"owner":"federation","region":"global"}`),
		}),
	}
}

func auditEventRows() []entityRow {
	return []entityRow{
		newEntityRow("1001", map[string]*kublingv1.Value{
			"id":             longValue(1001),
			"entity_type":    stringValue(taskEntityName),
			"entity_id":      stringValue("task-1"),
			"action":         charValue("U"),
			"sequence":       shortValue(1),
			"risk_score":     doubleValue(0.15),
			"occurred_at":    timestampValue("2026-08-02T09:15:30.125"),
			"source_address": varbinaryValue([]byte{127, 0, 0, 1}),
			"payload":        jsonValue(`{"completed":true}`),
			"raw_payload":    blobValue([]byte(`completed=true`)),
		}),
		newEntityRow("1002", map[string]*kublingv1.Value{
			"id":             longValue(1002),
			"entity_type":    stringValue(projectEntityName),
			"entity_id":      stringValue("project-2"),
			"action":         charValue("C"),
			"sequence":       shortValue(2),
			"risk_score":     doubleValue(0.05),
			"occurred_at":    timestampValue("2026-08-03T10:00:00"),
			"source_address": varbinaryValue([]byte{10, 0, 0, 42}),
			"payload":        jsonValue(`{"status":"P"}`),
			"raw_payload":    blobValue([]byte(`status=P`)),
		}),
	}
}

func typeSampleRows() []entityRow {
	pointWKB := []byte{
		1, 1, 0, 0, 0,
		0, 0, 0, 0, 0, 0, 240, 63,
		0, 0, 0, 0, 0, 0, 0, 64,
	}

	return []entityRow{
		newEntityRow("canonical", map[string]*kublingv1.Value{
			"sample_id":        stringValue("canonical"),
			"string_value":     stringValue("Kubling"),
			"varbinary_value":  varbinaryValue([]byte{0x4b, 0x55, 0x42}),
			"char_value":       charValue("K"),
			"boolean_value":    booleanValue(true),
			"byte_value":       byteValue(127),
			"short_value":      shortValue(32_767),
			"integer_value":    integerValue(2_147_483_647),
			"long_value":       longValue(9_223_372_036_854_775_000),
			"biginteger_value": bigintegerValue("123456789012345678901234567890"),
			"float_value":      floatValue(3.25),
			"double_value":     doubleValue(3.141592653589793),
			"bigdecimal_value": bigdecimalValue("1234567890.12345678901234567890"),
			"date_value":       dateValue("2026-08-03"),
			"time_value":       timeValue("14:30:15.125"),
			"timestamp_value":  timestampValue("2026-08-03T14:30:15.125"),
			"blob_value":       blobValue([]byte("binary large object")),
			"clob_value":       clobValue("character large object"),
			"geometry_value":   geometryValue(pointWKB),
			"geography_value":  geographyValue(pointWKB),
			"json_value":       jsonValue(`{"engine":"kubling","sample":true}`),
			"xml_value":        xmlValue(`<sample engine="kubling"/>`),
		}),
	}
}

func resolveEntity(
	entity *providerv1.EntityReference,
) (*entityDefinition, error) {
	if entity == nil || entity.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "entity is required")
	}

	for _, definition := range entityDefinitions {
		if equalName(definition.name, entity.GetName()) {
			return definition, nil
		}
	}

	return nil, status.Errorf(
		codes.NotFound,
		"entity %q was not found",
		entity.GetName(),
	)
}

func validateMutableTaskEntity(entity *providerv1.EntityReference) error {
	definition, err := resolveEntity(entity)
	if err != nil {
		return err
	}
	if !definition.mutable || !equalName(definition.name, taskEntityName) {
		return status.Errorf(
			codes.FailedPrecondition,
			"entity %q is read-only",
			definition.name,
		)
	}

	return nil
}

func (d *entityDefinition) fieldByName(name string) (entityField, bool) {
	for _, field := range d.fields {
		if equalName(field.name, name) {
			return field, true
		}
	}

	return entityField{}, false
}

func (s *store) snapshot(definition *entityDefinition) []entityRow {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if equalName(definition.name, taskEntityName) {
		rows := make([]entityRow, 0, len(s.tasks))
		for _, storedTask := range s.tasks {
			rows = append(rows, taskRow(storedTask))
		}

		return rows
	}

	rows := s.entities[definition.name]
	return append([]entityRow(nil), rows...)
}

func newEntityRow(
	key string,
	values map[string]*kublingv1.Value,
) entityRow {
	normalized := make(map[string]*kublingv1.Value, len(values))
	for name, value := range values {
		normalized[strings.ToLower(name)] = value
	}

	return entityRow{key: key, values: normalized}
}

func taskRow(storedTask task) entityRow {
	description := nullValue()
	if storedTask.Description != nil {
		description = clobValue(*storedTask.Description)
	}

	estimateHours := nullValue()
	if storedTask.EstimateHours != nil {
		estimateHours = floatValue(*storedTask.EstimateHours)
	}

	dueAt := nullValue()
	if storedTask.DueAt != nil {
		dueAt = timestampValue(*storedTask.DueAt)
	}

	return newEntityRow(storedTask.ID, map[string]*kublingv1.Value{
		"id":             stringValue(storedTask.ID),
		"project_id":     stringValue(storedTask.ProjectID),
		"title":          stringValue(storedTask.Title),
		"description":    description,
		"completed":      booleanValue(storedTask.Completed),
		"priority":       integerValue(storedTask.Priority),
		"estimate_hours": estimateHours,
		"due_at":         dueAt,
	})
}

func rowValue(row entityRow, fieldName string) (*kublingv1.Value, error) {
	value, found := row.values[strings.ToLower(fieldName)]
	if !found {
		return nil, fmt.Errorf("unknown field %q", fieldName)
	}

	return value, nil
}

func taskFieldByName(name string) (entityField, bool) {
	return entityDefinitions[1].fieldByName(name)
}

func taskTuple(
	storedTask task,
	fields []entityField,
) (*providerv1.Tuple, error) {
	row := taskRow(storedTask)
	values := make([]*kublingv1.Value, 0, len(fields))
	for _, field := range fields {
		value, err := rowValue(row, field.name)
		if err != nil {
			return nil, err
		}

		values = append(values, value)
	}

	return &providerv1.Tuple{Values: values}, nil
}

func providerFields(fields []entityField) []*providerv1.Field {
	result := make([]*providerv1.Field, 0, len(fields))
	for _, field := range fields {
		result = append(result, &providerv1.Field{
			Name: field.name,
			Type: field.valueType,
		})
	}

	return result
}

func nullValue() *kublingv1.Value {
	return &kublingv1.Value{
		Kind: &kublingv1.Value_NullValue{NullValue: &kublingv1.NullValue{}},
	}
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func varbinaryValue(value []byte) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_VarbinaryValue{VarbinaryValue: value}}
}

func charValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_CharValue{CharValue: value}}
}

func booleanValue(value bool) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BooleanValue{BooleanValue: value}}
}

func byteValue(value int32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_ByteValue{ByteValue: value}}
}

func shortValue(value int32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_ShortValue{ShortValue: value}}
}

func integerValue(value int32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_IntegerValue{IntegerValue: value}}
}

func longValue(value int64) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_LongValue{LongValue: value}}
}

func bigintegerValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BigintegerValue{BigintegerValue: value}}
}

func floatValue(value float32) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_FloatValue{FloatValue: value}}
}

func doubleValue(value float64) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_DoubleValue{DoubleValue: value}}
}

func bigdecimalValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BigdecimalValue{BigdecimalValue: value}}
}

func dateValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_DateValue{DateValue: value}}
}

func timeValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_TimeValue{TimeValue: value}}
}

func timestampValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_TimestampValue{TimestampValue: value}}
}

func blobValue(value []byte) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BlobValue{
		BlobValue: &kublingv1.BlobValue{Data: value},
	}}
}

func clobValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_ClobValue{
		ClobValue: &kublingv1.ClobValue{Data: value},
	}}
}

func geometryValue(value []byte) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_GeometryValue{GeometryValue: value}}
}

func geographyValue(value []byte) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_GeographyValue{GeographyValue: value}}
}

func jsonValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_JsonValue{JsonValue: value}}
}

func xmlValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_XmlValue{XmlValue: value}}
}

func equalName(left, right string) bool {
	return strings.EqualFold(left, right)
}
