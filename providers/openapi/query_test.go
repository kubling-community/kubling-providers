package openapi

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQueryReadsJSONRowsAndBatchesResults(t *testing.T) {
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/api/invoices" {
			t.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Accept") != "application/json" || request.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("request headers = %v", request.Header)
		}
		return jsonResponse(http.StatusOK, `{
  "data": {
    "items": [
      {
        "id": "inv-1",
        "active": true,
        "createdAt": "2026-08-13T10:15:30",
        "priority": 7,
        "amount": 12.50,
        "labels": ["new", "paid"],
        "note": null
      },
      {
        "id": "inv-2",
        "active": false,
        "createdAt": "2026-08-13T11:00:00",
        "priority": 8,
        "amount": 25,
        "labels": [],
        "note": "second"
      }
    ]
  }
}`), nil
	})

	connection := newTestConnection(t, "https://billing.example.test/api", client)
	batchSize := uint32(1)
	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity:    &providerv1.EntityReference{Name: "invoice", Namespace: "billing"},
		BatchSize: &batchSize,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	defer stream.Close()

	first, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	if len(first.Fields) != 7 || len(first.Tuples) != 1 {
		t.Fatalf("first batch = %d fields, %d tuples", len(first.Fields), len(first.Tuples))
	}
	values := first.Tuples[0].Values
	if len(values) != len(first.Fields) {
		t.Fatalf("first tuple values = %d, fields = %d", len(values), len(first.Fields))
	}
	if values[0].GetBooleanValue() != true || values[1].GetBigdecimalValue() != "12.50" || values[2].GetTimestampValue() != "2026-08-13T10:15:30" {
		t.Fatalf("first tuple scalar values = %#v", values)
	}
	if values[3].GetStringValue() != "inv-1" || values[4].GetJsonValue() != `["new","paid"]` || values[5].GetNullValue() == nil || values[6].GetIntegerValue() != 7 {
		t.Fatalf("first tuple values = %#v", values)
	}

	second, err := stream.Next(context.Background())
	if err != nil || len(second.Tuples) != 1 || second.Tuples[0].Values[5].GetStringValue() != "second" {
		t.Fatalf("second Next() = %#v, %v", second, err)
	}
	if _, err := stream.Next(context.Background()); err != io.EOF {
		t.Fatalf("third Next() error = %v, want EOF", err)
	}
}

func TestProviderHTTPClientAllowsOnlySameOriginRedirects(t *testing.T) {
	connection := newTestConnection(t, "https://billing.example.test", testHTTPClient(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, invoicePage("inv-1")), nil
	}))
	initial := redirectTestRequest(t, "https://billing.example.test/invoices")

	if err := connection.provider.client.CheckRedirect(
		redirectTestRequest(t, "https://billing.example.test/redirected/invoices"),
		[]*http.Request{initial},
	); err != nil {
		t.Fatalf("same-origin redirect error = %v", err)
	}

	for _, target := range []string{
		"https://attacker.example.test/invoices",
		"http://billing.example.test/invoices",
		"https://billing.example.test:8443/invoices",
	} {
		err := connection.provider.client.CheckRedirect(
			redirectTestRequest(t, target),
			[]*http.Request{initial},
		)
		if err == nil || !strings.Contains(err.Error(), "redirect changed origin") {
			t.Errorf("redirect to %q error = %v, want changed origin", target, err)
		}
	}
}

func TestOperationURLRejectsOriginChanges(t *testing.T) {
	for _, operationPath := range []string{
		"https://attacker.example.test/invoices",
		"http://billing.example.test/invoices",
	} {
		if _, err := operationURL("https://billing.example.test/api", operationPath); err == nil || !strings.Contains(err.Error(), "changed datasource origin") {
			t.Errorf("operationURL(%q) error = %v, want origin rejection", operationPath, err)
		}
	}

	endpoint, err := operationURL("https://billing.example.test/api", "/invoices")
	if err != nil || endpoint != "https://billing.example.test/api/invoices" {
		t.Fatalf("operationURL() = %q, %v", endpoint, err)
	}
}

func TestDatasourceTransportRequiresExplicitHTTPOptInForCredentials(t *testing.T) {
	config := Config{
		BaseURL: "http://api.example.test",
		Headers: map[string]string{"X-API-Key": "secret"},
	}
	if err := validateDatasourceTransport(config); err == nil || !strings.Contains(err.Error(), "allowInsecureHttp must be true") {
		t.Fatalf("validateDatasourceTransport() error = %v, want explicit opt-in", err)
	}
	config.AllowInsecureHTTP = true
	if err := validateDatasourceTransport(config); err != nil {
		t.Fatalf("validateDatasourceTransport() opt-in error = %v", err)
	}
}

func redirectTestRequest(t *testing.T, rawURL string) *http.Request {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error = %v", rawURL, err)
	}
	return &http.Request{URL: parsed, Header: make(http.Header)}
}

func TestQueryRejectsResponseLargerThanConfiguredLimit(t *testing.T) {
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, invoicePage("invoice-with-a-long-identifier")), nil
	})
	connection := newTestConnection(t, "https://billing.example.test", client)
	connection.provider.config.MaxResponseBytes = 32

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if _, err := stream.Next(context.Background()); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("Next() error = %v, want ResourceExhausted", err)
	}
}

func TestQuerySupportsFieldAndLiteralProjections(t *testing.T) {
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, `{"data":{"items":[{"id":"inv-1","active":true,"createdAt":"2026-08-13T10:15:30"}]}}`), nil
	})
	connection := newTestConnection(t, "https://billing.example.test", client)

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
		Projections: []*providerv1.Projection{
			{
				Expression: &providerv1.Expression{Kind: &providerv1.Expression_Field{Field: &providerv1.FieldReference{Name: "id"}}},
				OutputName: "invoice_id",
			},
			{
				Expression: &providerv1.Expression{Kind: &providerv1.Expression_Literal{Literal: &providerv1.Literal{Value: &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: "openapi"}}}}},
				OutputName: "source",
			},
		},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if batch.Fields[0].Name != "invoice_id" || batch.Fields[1].Name != "source" || batch.Tuples[0].Values[0].GetStringValue() != "inv-1" || batch.Tuples[0].Values[1].GetStringValue() != "openapi" {
		t.Fatalf("projected batch = %#v", batch)
	}
}

func TestQueryRejectsOrderingBeforeHTTP(t *testing.T) {
	requested := false
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		requested = true
		return jsonResponse(http.StatusOK, `{}`), nil
	})
	connection := newTestConnection(t, "https://billing.example.test", client)

	_, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity:  &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
		OrderBy: []*providerv1.OrderBy{{}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Query() error = %v, want InvalidArgument", err)
	}
	if requested {
		t.Fatal("Query() executed HTTP request for rejected ordering")
	}
}

func TestQueryAppliesStaticParametersAndEqualityFilters(t *testing.T) {
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		query := request.URL.Query()
		if query.Get("jql") != "project = KBL" || query.Get("fields") != "id,active" || query.Get("status") != "OPEN" || query.Get("active") != "true" {
			t.Errorf("request query = %v", query)
		}
		return jsonResponse(http.StatusOK, invoicePage("inv-1")), nil
	})
	connection := newTestConnectionWithEntity(t, "https://billing.example.test", client, EntityConfig{
		Name:          "INVOICE",
		ListOperation: "listInvoices",
		ResponsePath:  "/data/items",
		QueryParameters: []QueryParameterConfig{
			{Name: "jql", Value: "project = KBL"},
			{Name: "fields", Value: "id,active"},
		},
		EqualityFilters: []EqualityFilterConfig{
			{Field: "id", Parameter: "status"},
			{Field: "active", Parameter: "active"},
		},
	})

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
		Filter: logicalAND(
			equality(literalString("OPEN"), field("id")),
			equality(field("active"), literalBoolean(true)),
		),
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if _, err := stream.Next(context.Background()); err != nil {
		t.Fatalf("Next() error = %v", err)
	}
}

func TestQueryRejectsUnboundFilterBeforeHTTP(t *testing.T) {
	requested := false
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		requested = true
		return jsonResponse(http.StatusOK, invoicePage()), nil
	})
	connection := newTestConnection(t, "https://billing.example.test", client)

	_, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
		Filter: equality(field("id"), literalString("inv-1")),
	})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "no equality filter binding") {
		t.Fatalf("Query() error = %v, want unbound filter InvalidArgument", err)
	}
	if requested {
		t.Fatal("Query() executed HTTP request for rejected filter")
	}
}

func TestQueryRequiresFilterBoundOpenAPIParameter(t *testing.T) {
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	spec := strings.Replace(
		testOpenAPISpec,
		"        - name: status\n          in: query",
		"        - name: status\n          in: query\n          required: true",
		1,
	)
	writeTestFile(t, specPath, spec)
	provider, err := New(Config{
		SpecFile:  specPath,
		BaseURL:   "https://billing.example.test",
		Namespace: "billing",
		Entities: []EntityConfig{{
			Name:            "INVOICE",
			ListOperation:   "listInvoices",
			ResponsePath:    "/data/items",
			EqualityFilters: []EqualityFilterConfig{{Field: "id", Parameter: "status"}},
		}},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	connection, err := provider.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	_, err = connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
	})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "requires an equality filter") {
		t.Fatalf("Query() error = %v, want required filter InvalidArgument", err)
	}
}

func TestQueryAppliesLimitAndOffsetLocally(t *testing.T) {
	requestCount := 0
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		requestCount++
		return jsonResponse(http.StatusOK, invoicePage("inv-1", "inv-2", "inv-3", "inv-4")), nil
	})
	connection := newTestConnection(t, "https://billing.example.test", client)
	limit := uint64(2)
	offset := uint64(1)
	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity:      &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
		Projections: []*providerv1.Projection{{Expression: field("id")}},
		Limit:       &limit,
		Offset:      &offset,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if requestCount != 1 || len(batch.Tuples) != 2 || batch.Tuples[0].Values[0].GetStringValue() != "inv-2" || batch.Tuples[1].Values[0].GetStringValue() != "inv-3" {
		t.Fatalf("limited batch = %#v, requests = %d", batch, requestCount)
	}
	if _, err := stream.Next(context.Background()); err != io.EOF {
		t.Fatalf("second Next() error = %v, want EOF", err)
	}
}

func TestQueryWithZeroLimitDoesNotCallHTTP(t *testing.T) {
	requested := false
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		requested = true
		return jsonResponse(http.StatusOK, invoicePage("inv-1")), nil
	})
	connection := newTestConnection(t, "https://billing.example.test", client)
	limit := uint64(0)
	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
		Limit:  &limit,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if _, err := stream.Next(context.Background()); err != io.EOF {
		t.Fatalf("Next() error = %v, want EOF", err)
	}
	if requested {
		t.Fatal("zero-limit query executed HTTP request")
	}
}

func TestQueryMapsHTTPFailure(t *testing.T) {
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusServiceUnavailable, "temporarily unavailable"), nil
	})
	connection := newTestConnection(t, "https://billing.example.test", client)

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
	})
	if err == nil {
		_, err = stream.Next(context.Background())
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Query() error = %v, want Unavailable", err)
	}
}

func TestQueryFollowsOffsetPagination(t *testing.T) {
	var requests []string
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		requests = append(requests, request.URL.RawQuery)
		switch request.URL.Query().Get("offset") {
		case "0":
			return jsonResponse(http.StatusOK, invoicePage("inv-1", "inv-2")), nil
		case "2":
			return jsonResponse(http.StatusOK, invoicePage("inv-3")), nil
		default:
			t.Fatalf("unexpected offset query %q", request.URL.RawQuery)
			return nil, nil
		}
	})
	connection := newTestConnectionWithEntity(t, "https://billing.example.test", client, EntityConfig{
		Name:          "INVOICE",
		ListOperation: "listInvoices",
		ResponsePath:  "/data/items",
		Pagination: &PaginationConfig{
			Mode:              PaginationModeOffset,
			PageSize:          2,
			PageSizeParameter: "limit",
			OffsetParameter:   "offset",
		},
	})

	batch := querySingleBatch(t, connection)
	if len(batch.Tuples) != 3 {
		t.Fatalf("paginated tuples = %d, want 3", len(batch.Tuples))
	}
	if len(requests) != 2 || requests[0] != "limit=2&offset=0" || requests[1] != "limit=2&offset=2" {
		t.Fatalf("pagination queries = %v", requests)
	}
}

func TestQueryStartsOffsetPaginationAtSQLRequestOffset(t *testing.T) {
	var offsets []string
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		offset := request.URL.Query().Get("offset")
		offsets = append(offsets, offset)
		if offset != "2" {
			t.Fatalf("offset = %q, want 2", offset)
		}
		return jsonResponse(http.StatusOK, invoicePage("inv-3")), nil
	})
	connection := newTestConnectionWithEntity(t, "https://billing.example.test", client, EntityConfig{
		Name:          "INVOICE",
		ListOperation: "listInvoices",
		ResponsePath:  "/data/items",
		Pagination: &PaginationConfig{
			Mode:              PaginationModeOffset,
			PageSize:          2,
			PageSizeParameter: "limit",
			OffsetParameter:   "offset",
		},
	})
	limit := uint64(1)
	offset := uint64(2)
	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity:      &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
		Projections: []*providerv1.Projection{{Expression: field("id")}},
		Limit:       &limit,
		Offset:      &offset,
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	batch, err := stream.Next(context.Background())
	if err != nil || len(batch.Tuples) != 1 || batch.Tuples[0].Values[0].GetStringValue() != "inv-3" {
		t.Fatalf("Next() = %#v, %v", batch, err)
	}
	if len(offsets) != 1 {
		t.Fatalf("offset requests = %v", offsets)
	}
}

func TestQueryFollowsPagePaginationFromZero(t *testing.T) {
	startPage := uint64(0)
	var pages []string
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		page := request.URL.Query().Get("page")
		pages = append(pages, page)
		if page == "0" {
			return jsonResponse(http.StatusOK, invoicePage("inv-1", "inv-2")), nil
		}
		return jsonResponse(http.StatusOK, invoicePage()), nil
	})
	connection := newTestConnectionWithEntity(t, "https://billing.example.test", client, EntityConfig{
		Name:          "INVOICE",
		ListOperation: "listInvoices",
		ResponsePath:  "/data/items",
		Pagination: &PaginationConfig{
			Mode:              PaginationModePage,
			PageSize:          2,
			PageSizeParameter: "per_page",
			PageParameter:     "page",
			StartPage:         &startPage,
		},
	})

	batch := querySingleBatch(t, connection)
	if len(batch.Tuples) != 2 || len(pages) != 2 || pages[0] != "0" || pages[1] != "1" {
		t.Fatalf("page pagination tuples = %d, pages = %v", len(batch.Tuples), pages)
	}
}

func TestQueryFollowsCursorPagination(t *testing.T) {
	var cursors []string
	client := testHTTPClient(func(request *http.Request) (*http.Response, error) {
		cursor := request.URL.Query().Get("after")
		cursors = append(cursors, cursor)
		if cursor == "" {
			return jsonResponse(http.StatusOK, invoiceCursorPage("next-1", "inv-1")), nil
		}
		return jsonResponse(http.StatusOK, invoiceCursorPage("", "inv-2")), nil
	})
	connection := newTestConnectionWithEntity(t, "https://billing.example.test", client, EntityConfig{
		Name:          "INVOICE",
		ListOperation: "listInvoices",
		ResponsePath:  "/data/items",
		Pagination: &PaginationConfig{
			Mode:              PaginationModeCursor,
			PageSize:          1,
			PageSizeParameter: "limit",
			CursorParameter:   "after",
			NextCursorPath:    "/data/next",
		},
	})

	batch := querySingleBatch(t, connection)
	if len(batch.Tuples) != 2 || len(cursors) != 2 || cursors[0] != "" || cursors[1] != "next-1" {
		t.Fatalf("cursor pagination tuples = %d, cursors = %v", len(batch.Tuples), cursors)
	}
}

func TestQueryStopsWhenCursorPropertyIsOmitted(t *testing.T) {
	requestCount := 0
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		requestCount++
		return jsonResponse(http.StatusOK, `{"data":{"items":[{"id":"inv-1","active":true,"createdAt":"2026-08-13T10:15:30"}]}}`), nil
	})
	connection := newTestConnectionWithEntity(t, "https://billing.example.test", client, EntityConfig{
		Name:          "INVOICE",
		ListOperation: "listInvoices",
		ResponsePath:  "/data/items",
		Pagination: &PaginationConfig{
			Mode:            PaginationModeCursor,
			PageSize:        1,
			CursorParameter: "after",
			NextCursorPath:  "/data/next",
		},
	})

	batch := querySingleBatch(t, connection)
	if requestCount != 1 || len(batch.Tuples) != 1 {
		t.Fatalf("cursor result = %d requests, %d tuples", requestCount, len(batch.Tuples))
	}
}

func TestQueryStopsWhenHasMoreIsFalse(t *testing.T) {
	requestCount := 0
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		requestCount++
		return jsonResponse(http.StatusOK, `{"data":{"items":[{"id":"inv-1","active":true,"createdAt":"2026-08-13T10:15:30"}],"next":"unused","hasMore":false}}`), nil
	})
	connection := newTestConnectionWithEntity(t, "https://billing.example.test", client, EntityConfig{
		Name:          "INVOICE",
		ListOperation: "listInvoices",
		ResponsePath:  "/data/items",
		Pagination: &PaginationConfig{
			Mode:            PaginationModeCursor,
			PageSize:        1,
			CursorParameter: "after",
			NextCursorPath:  "/data/next",
			HasMorePath:     "/data/hasMore",
		},
	})

	batch := querySingleBatch(t, connection)
	if requestCount != 1 || len(batch.Tuples) != 1 {
		t.Fatalf("cursor result = %d requests, %d tuples", requestCount, len(batch.Tuples))
	}
}

func TestQueryRejectsRepeatedCursor(t *testing.T) {
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, invoiceCursorPage("repeated", "inv-1")), nil
	})
	connection := newTestConnectionWithEntity(t, "https://billing.example.test", client, EntityConfig{
		Name:          "INVOICE",
		ListOperation: "listInvoices",
		ResponsePath:  "/data/items",
		Pagination: &PaginationConfig{
			Mode:            PaginationModeCursor,
			PageSize:        1,
			CursorParameter: "after",
			NextCursorPath:  "/data/next",
		},
	})

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	first, err := stream.Next(context.Background())
	if err != nil || len(first.Tuples) != 1 {
		t.Fatalf("first Next() = %#v, %v", first, err)
	}
	if _, err := stream.Next(context.Background()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second Next() error = %v, want FailedPrecondition", err)
	}
}

func TestQueryEnforcesMaximumPages(t *testing.T) {
	client := testHTTPClient(func(*http.Request) (*http.Response, error) {
		return jsonResponse(http.StatusOK, invoicePage("inv-1")), nil
	})
	connection := newTestConnectionWithEntity(t, "https://billing.example.test", client, EntityConfig{
		Name:          "INVOICE",
		ListOperation: "listInvoices",
		ResponsePath:  "/data/items",
		Pagination: &PaginationConfig{
			Mode:            PaginationModeOffset,
			PageSize:        1,
			OffsetParameter: "offset",
			MaxPages:        1,
		},
	})

	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	first, err := stream.Next(context.Background())
	if err != nil || len(first.Tuples) != 1 {
		t.Fatalf("first Next() = %#v, %v", first, err)
	}
	if _, err := stream.Next(context.Background()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second Next() error = %v, want FailedPrecondition", err)
	}
}

func newTestConnection(t *testing.T, baseURL string, client *http.Client) *Connection {
	return newTestConnectionWithEntity(t, baseURL, client, EntityConfig{
		Name:          "INVOICE",
		ListOperation: "listInvoices",
		ResponsePath:  "/data/items",
		PrimaryKey:    []string{"id"},
	})
}

func newTestConnectionWithEntity(t *testing.T, baseURL string, client *http.Client, entity EntityConfig) *Connection {
	t.Helper()
	specPath := filepath.Join(t.TempDir(), "api.yaml")
	writeTestFile(t, specPath, testOpenAPISpec)
	provider, err := New(Config{
		SpecFile:   specPath,
		BaseURL:    baseURL,
		HTTPClient: client,
		Namespace:  "billing",
		Headers:    map[string]string{"Authorization": "Bearer test-token"},
		Entities:   []EntityConfig{entity},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	connection, err := provider.Open(context.Background())
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return connection.(*Connection)
}

func querySingleBatch(t *testing.T, connection *Connection) *providerv1.TupleBatch {
	t.Helper()
	stream, err := connection.Query(context.Background(), &providerv1.QueryRequest{
		Entity: &providerv1.EntityReference{Name: "INVOICE", Namespace: "billing"},
	})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	batch, err := stream.Next(context.Background())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	return batch
}

func invoicePage(ids ...string) string {
	return invoiceCursorPage("", ids...)
}

func invoiceCursorPage(next string, ids ...string) string {
	rows := make([]string, 0, len(ids))
	for _, id := range ids {
		rows = append(rows, `{"id":`+strconv.Quote(id)+`,"active":true,"createdAt":"2026-08-13T10:15:30"}`)
	}
	return `{"data":{"items":[` + strings.Join(rows, ",") + `],"next":` + strconv.Quote(next) + `}}`
}

func field(name string) *providerv1.Expression {
	return &providerv1.Expression{
		Kind: &providerv1.Expression_Field{Field: &providerv1.FieldReference{Name: name}},
	}
}

func literalString(value string) *providerv1.Expression {
	return &providerv1.Expression{
		Kind: &providerv1.Expression_Literal{Literal: &providerv1.Literal{
			Value: &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}},
		}},
	}
}

func literalBoolean(value bool) *providerv1.Expression {
	return &providerv1.Expression{
		Kind: &providerv1.Expression_Literal{Literal: &providerv1.Literal{
			Value: &kublingv1.Value{Kind: &kublingv1.Value_BooleanValue{BooleanValue: value}},
		}},
	}
}

func equality(left, right *providerv1.Expression) *providerv1.Expression {
	return &providerv1.Expression{
		Kind: &providerv1.Expression_Comparison{Comparison: &providerv1.ComparisonExpression{
			Operator: providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			Left:     left,
			Right:    right,
		}},
	}
}

func logicalAND(operands ...*providerv1.Expression) *providerv1.Expression {
	return &providerv1.Expression{
		Kind: &providerv1.Expression_Logical{Logical: &providerv1.LogicalExpression{
			Operator: providerv1.LogicalOperator_LOGICAL_OPERATOR_AND,
			Operands: operands,
		}},
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func testHTTPClient(function roundTripFunc) *http.Client {
	return &http.Client{Transport: function}
}

func jsonResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
