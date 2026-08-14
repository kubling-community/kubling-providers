package openapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMutationMetadataAndCapabilitiesFollowExplicitMappings(t *testing.T) {
	provider := newMutationTestProvider(t, testHTTPClient(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusNoContent, ""), nil
	}))

	capabilities, err := provider.Capabilities(context.Background())
	if err != nil {
		t.Fatalf("Capabilities() error = %v", err)
	}
	if capabilities.Mutations == nil || !capabilities.Mutations.Insert || !capabilities.Mutations.Update || !capabilities.Mutations.Delete || capabilities.Mutations.GeneratedValues {
		t.Fatalf("mutation capabilities = %#v", capabilities.Mutations)
	}

	metadata, err := provider.Metadata(context.Background())
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	table := metadata.Tables[0]
	if !table.GetUpdatable() || table.Properties["openapi.insert.operation_id"] != "createInvoice" || table.Properties["openapi.update.method"] != http.MethodPatch || table.Properties["openapi.delete.path"] != "/invoices/{invoiceId}" {
		t.Fatalf("mutation table metadata = %#v", table)
	}
	columns := make(map[string]*providerv1.ColumnMetadata)
	for _, column := range table.Columns {
		columns[column.Name] = column
	}
	if columns["id"].GetUpdatable() || !columns["customerId"].GetUpdatable() || !columns["amount"].GetUpdatable() || !columns["status"].GetUpdatable() {
		t.Fatalf("column mutation metadata = %#v", columns)
	}
	if columns["customerId"].GetNullable() || columns["amount"].GetNullable() || !columns["id"].GetNullable() || !columns["status"].GetNullable() {
		t.Fatalf("column insert nullability = %#v", columns)
	}
}

func TestInsertExecutesOneJSONRequestPerTuple(t *testing.T) {
	requestCount := 0
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		requestCount++
		if request.Method != http.MethodPost || request.URL.Path != "/api/invoices" || request.URL.Query().Get("tenant") != "tenant-a" {
			t.Errorf("request = %s %s?%s", request.Method, request.URL.Path, request.URL.RawQuery)
		}
		if request.Header.Get("Content-Type") != "application/json" || request.Header.Get("X-Provider-Test") != "true" {
			t.Errorf("request headers = %v", request.Header)
		}
		var body map[string]any
		decoder := json.NewDecoder(request.Body)
		decoder.UseNumber()
		if err := decoder.Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		payload := body["payload"].(map[string]any)
		if payload["customerId"] != "customer-1" || payload["amount"].(json.Number).String() != "12.50" {
			t.Errorf("request body = %#v", body)
		}
		return jsonResponse(http.StatusCreated, `{}`), nil
	})
	connection := newMutationTestConnection(t, client)

	response, err := connection.Insert(context.Background(), &providerv1.InsertRequest{
		Entity: mutationEntity(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{
				{Name: "customerId", Type: kublingv1.ValueType_VALUE_TYPE_STRING},
				{Name: "amount", Type: kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL},
			},
			Tuples: []*providerv1.Tuple{
				{Values: []*kublingv1.Value{stringValue("customer-1"), decimalValue("12.50")}},
				{Values: []*kublingv1.Value{stringValue("customer-1"), decimalValue("12.50")}},
			},
		},
	})
	if err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if response.GetAffectedRows() != 2 || requestCount != 2 {
		t.Fatalf("Insert() affected = %d, requests = %d", response.GetAffectedRows(), requestCount)
	}
}

func TestInsertRejectsMissingRequiredBodyFieldBeforeHTTP(t *testing.T) {
	requested := false
	connection := newMutationTestConnection(t, testHTTPClient(func(*http.Request) (*http.Response, error) {
		requested = true
		return jsonResponse(http.StatusCreated, `{}`), nil
	}))

	_, err := connection.Insert(context.Background(), &providerv1.InsertRequest{
		Entity: mutationEntity(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{{Name: "status", Type: kublingv1.ValueType_VALUE_TYPE_STRING}},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{stringValue("OPEN")}}},
		},
	})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "required body fields are missing: amount, customerId") {
		t.Fatalf("Insert() error = %v, want required field InvalidArgument", err)
	}
	if requested {
		t.Fatal("Insert() called HTTP for an invalid tuple")
	}
}

func TestUpdateBindsPathFilterAndSendsPatchBody(t *testing.T) {
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodPatch || request.URL.EscapedPath() != "/api/invoices/inv%201" {
			t.Errorf("request = %s %s", request.Method, request.URL.EscapedPath())
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("read request body: %v", err)
		}
		if string(body) != `{"payload":{"status":"PAID"}}` {
			t.Errorf("request body = %s", body)
		}
		return jsonResponse(http.StatusOK, `{}`), nil
	})
	connection := newMutationTestConnection(t, client)

	response, err := connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity: mutationEntity(),
		Assignments: []*providerv1.Assignment{{
			Field: "status",
			Value: literalString("PAID"),
		}},
		Filter: equality(field("id"), literalString("inv 1")),
	})
	if err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if response.GetAffectedRows() != 1 {
		t.Fatalf("Update() affected = %d", response.GetAffectedRows())
	}
}

func TestDeleteBindsPathFilter(t *testing.T) {
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodDelete || request.URL.Path != "/api/invoices/inv-1" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		if request.Body != nil {
			t.Errorf("delete body is not nil")
		}
		return jsonResponse(http.StatusNoContent, ""), nil
	})
	connection := newMutationTestConnection(t, client)

	response, err := connection.Delete(context.Background(), &providerv1.DeleteRequest{
		Entity: mutationEntity(),
		Filter: equality(field("id"), literalString("inv-1")),
	})
	if err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if response.GetAffectedRows() != 1 {
		t.Fatalf("Delete() affected = %d", response.GetAffectedRows())
	}
}

func TestUpdateRejectsFilterNotBoundToPath(t *testing.T) {
	requested := false
	connection := newMutationTestConnection(t, testHTTPClient(func(*http.Request) (*http.Response, error) {
		requested = true
		return jsonResponse(http.StatusOK, `{}`), nil
	}))

	_, err := connection.Update(context.Background(), &providerv1.UpdateRequest{
		Entity:      mutationEntity(),
		Assignments: []*providerv1.Assignment{{Field: "status", Value: literalString("PAID")}},
		Filter: logicalAND(
			equality(field("id"), literalString("inv-1")),
			equality(field("status"), literalString("OPEN")),
		),
	})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "is not bound to the operation path") {
		t.Fatalf("Update() error = %v, want unsafe filter InvalidArgument", err)
	}
	if requested {
		t.Fatal("Update() called HTTP for an unsafe filter")
	}
}

func TestMutationMapsHTTPFailures(t *testing.T) {
	connection := newMutationTestConnection(t, testHTTPClient(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusTooManyRequests, "rate limited"), nil
	}))

	_, err := connection.Delete(context.Background(), &providerv1.DeleteRequest{
		Entity: mutationEntity(),
		Filter: equality(field("id"), literalString("inv-1")),
	})
	if status.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), "rate limited") {
		t.Fatalf("Delete() error = %v, want ResourceExhausted", err)
	}
}

func TestNewRejectsUnsafeMutationMappings(t *testing.T) {
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	writeTestFile(t, specPath, mutationOpenAPISpec)
	tests := []struct {
		name   string
		change func(*EntityMutationConfig)
		want   string
	}{
		{
			name: "insert method",
			change: func(mutations *EntityMutationConfig) {
				mutations.Insert.Operation = "updateInvoice"
			},
			want: "uses PATCH, want POST",
		},
		{
			name: "unbound delete path",
			change: func(mutations *EntityMutationConfig) {
				mutations.Delete.PathParameters = nil
			},
			want: `path parameter "invoiceId" has no field or static binding`,
		},
		{
			name: "static update target",
			change: func(mutations *EntityMutationConfig) {
				mutations.Update.PathParameters[0] = PathParameterConfig{Parameter: "invoiceId", Value: "fixed"}
			},
			want: "require at least one field-bound path parameter",
		},
		{
			name: "unknown query parameter",
			change: func(mutations *EntityMutationConfig) {
				mutations.Insert.QueryParameters[0].Name = "missing"
			},
			want: `query parameter "missing" is not defined`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := mutationTestConfig(specPath, nil)
			test.change(config.Entities[0].Mutations)
			_, err := New(config)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("New() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func newMutationTestProvider(t *testing.T, client *http.Client) *Provider {
	t.Helper()
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	writeTestFile(t, specPath, mutationOpenAPISpec)
	provider, err := New(mutationTestConfig(specPath, client))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return provider
}

func mutationTestConfig(specPath string, client *http.Client) Config {
	return Config{
		SpecFile:   specPath,
		BaseURL:    "https://billing.example.test/api",
		Namespace:  "billing",
		HTTPClient: client,
		Headers:    map[string]string{"X-Provider-Test": "true"},
		Entities: []EntityConfig{{
			Name:          "INVOICE",
			ListOperation: "listInvoices",
			PrimaryKey:    []string{"id"},
			Mutations: &EntityMutationConfig{
				Insert: &MutationOperationConfig{
					Operation:       "createInvoice",
					QueryParameters: []QueryParameterConfig{{Name: "tenant", Value: "tenant-a"}},
					BodyPath:        "/payload",
				},
				Update: &MutationOperationConfig{
					Operation:      "updateInvoice",
					PathParameters: []PathParameterConfig{{Parameter: "invoiceId", Field: "id"}},
					BodyPath:       "/payload",
				},
				Delete: &MutationOperationConfig{
					Operation:      "deleteInvoice",
					PathParameters: []PathParameterConfig{{Parameter: "invoiceId", Field: "id"}},
				},
			},
		}},
	}
}

func newMutationTestConnection(t *testing.T, client *http.Client) *Connection {
	t.Helper()
	connection, err := newMutationTestProvider(t, client).Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return connection.(*Connection)
}

func mutationEntity() *providerv1.EntityReference {
	return &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"}
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func decimalValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BigdecimalValue{BigdecimalValue: value}}
}

const mutationOpenAPISpec = `
openapi: 3.1.0
info:
  title: Generic Billing API
  version: 1.0.0
paths:
  /invoices:
    get:
      operationId: listInvoices
      responses:
        "200":
          description: Invoices
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "#/components/schemas/Invoice"
    post:
      operationId: createInvoice
      parameters:
        - name: tenant
          in: query
          required: true
          schema:
            type: string
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [payload]
              properties:
                payload:
                  $ref: "#/components/schemas/InvoiceCreate"
      responses:
        "201":
          description: Created
  /invoices/{invoiceId}:
    parameters:
      - name: invoiceId
        in: path
        required: true
        schema:
          type: string
    patch:
      operationId: updateInvoice
      requestBody:
        required: true
        content:
          application/json:
            schema:
              type: object
              required: [payload]
              properties:
                payload:
                  $ref: "#/components/schemas/InvoicePatch"
      responses:
        "200":
          description: Updated
    delete:
      operationId: deleteInvoice
      responses:
        "204":
          description: Deleted
components:
  schemas:
    Invoice:
      type: object
      required: [id, customerId, amount, status]
      properties:
        id:
          type: string
          readOnly: true
        customerId:
          type: string
        amount:
          type: number
        status:
          type: string
    InvoiceCreate:
      type: object
      required: [customerId, amount]
      properties:
        customerId:
          type: string
        amount:
          type: number
        status:
          type: string
    InvoicePatch:
      type: object
      properties:
        amount:
          type: number
        status:
          type: string
`
