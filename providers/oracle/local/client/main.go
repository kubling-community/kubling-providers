package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"time"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultAddress     = "127.0.0.1:50051"
	resourceManagerID  = "oracle-local"
	rollbackRecordName = "local rollback row"
	commitRecordName   = "local commit row"
	recoveryRecordName = "xa recovery row"
)

var branchID = []byte("kubling-oracle-local-recovery-v1")

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	mode := flag.String("mode", "basic", "basic, prepare or recover")
	address := flag.String("address", env("KUBLING_ORACLE_TEST_TARGET", defaultAddress), "provider address")
	flag.Parse()

	connection, client, err := connect(*address)
	if err != nil {
		return err
	}
	defer connection.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	switch *mode {
	case "basic":
		return runBasic(ctx, client)
	case "prepare":
		return runPrepare(ctx, client)
	case "recover":
		return runRecover(ctx, client)
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}
}

func connect(address string) (*grpc.ClientConn, providerv1.ProviderServiceClient, error) {
	connection, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("create provider client: %w", err)
	}
	client := providerv1.NewProviderServiceClient(connection)
	deadline := time.Now().Add(90 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		health, healthErr := client.Health(ctx, &providerv1.HealthRequest{})
		cancel()
		if healthErr == nil && health.GetHealthy() {
			return connection, client, nil
		}
		if time.Now().After(deadline) {
			connection.Close()
			if healthErr != nil {
				return nil, nil, fmt.Errorf("wait for provider health: %w", healthErr)
			}
			return nil, nil, fmt.Errorf("provider remained unhealthy: %s", health.GetMessage())
		}
		time.Sleep(time.Second)
	}
}

func runBasic(ctx context.Context, client providerv1.ProviderServiceClient) error {
	capabilities, err := client.GetCapabilities(ctx, &providerv1.GetCapabilitiesRequest{})
	if err != nil {
		return fmt.Errorf("get capabilities: %w", err)
	}
	transactions := capabilities.GetTransactions()
	if !transactions.GetSupported() || !transactions.GetRecovery().GetSupported() {
		return errors.New("provider did not advertise local and durable transaction support")
	}
	if transactions.GetRecovery().GetResourceManagerId() != resourceManagerID {
		return fmt.Errorf(
			"resource manager = %q, want %q",
			transactions.GetRecovery().GetResourceManagerId(),
			resourceManagerID)
	}

	schema, err := client.GetSchema(ctx, &providerv1.GetSchemaRequest{})
	if err != nil {
		return fmt.Errorf("get schema: %w", err)
	}
	if err := requireTables(
		schema.GetMetadata(),
		"Order",
		"Order Summary",
		"Telemetry_%",
		"REGIONS",
		"CUSTOMERS",
		"PRODUCTS",
		"SALES_ORDERS",
		"SALES_ORDER_LINES",
		"SALES_ORDER_DETAIL",
	); err != nil {
		return err
	}
	if err := requireWritableTables(schema.GetMetadata(), "Order", "Order Summary", "Telemetry_%"); err != nil {
		return err
	}
	if err := requireReadOnlyTables(schema.GetMetadata(), "SALES_ORDER_DETAIL"); err != nil {
		return err
	}

	connectionID, err := openConnection(ctx, client)
	if err != nil {
		return err
	}
	defer closeConnection(context.Background(), client, connectionID)

	for _, name := range []string{rollbackRecordName, commitRecordName, recoveryRecordName} {
		if _, err := client.Delete(ctx, deleteByName(connectionID, name)); err != nil {
			return fmt.Errorf("clean fixture row %q: %w", name, err)
		}
	}

	count, err := countOrders(ctx, client, connectionID, nil)
	if err != nil {
		return err
	}
	if count != 2 {
		return fmt.Errorf("initial order count = %d, want 2", count)
	}
	detailCount, err := countRows(ctx, client, connectionID, salesOrderDetailEntity(), nil)
	if err != nil {
		return fmt.Errorf("count sales order detail rows: %w", err)
	}
	if detailCount != 6000 {
		return fmt.Errorf("sales order detail count = %d, want 6000", detailCount)
	}
	euWestCount, err := countRows(
		ctx,
		client,
		connectionID,
		salesOrderDetailEntity(),
		comparison(
			providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			field("REGION_CODE"),
			stringLiteral("EU-WEST")),
	)
	if err != nil {
		return fmt.Errorf("count EU-WEST sales order detail rows: %w", err)
	}
	if euWestCount != 1500 {
		return fmt.Errorf("EU-WEST sales order detail count = %d, want 1500", euWestCount)
	}

	rows, err := query(ctx, client, orderedPage(connectionID))
	if err != nil {
		return fmt.Errorf("query ordered page: %w", err)
	}
	if len(rows) != 1 || rows[0][0].GetStringValue() != "first order" {
		return fmt.Errorf("ordered page returned unexpected rows: %v", rows)
	}

	if _, err := client.BeginTransaction(ctx, &providerv1.BeginTransactionRequest{
		ConnectionId: connectionID,
	}); err != nil {
		return fmt.Errorf("begin rollback transaction: %w", err)
	}
	if _, err := client.Insert(ctx, insertOrder(connectionID, rollbackRecordName, "7.50")); err != nil {
		return fmt.Errorf("insert rollback row: %w", err)
	}
	if _, err := client.RollbackTransaction(ctx, &providerv1.RollbackTransactionRequest{
		ConnectionId: connectionID,
	}); err != nil {
		return fmt.Errorf("roll back local transaction: %w", err)
	}
	if err := requireOrderCount(ctx, client, connectionID, rollbackRecordName, 0); err != nil {
		return err
	}

	if _, err := client.BeginTransaction(ctx, &providerv1.BeginTransactionRequest{
		ConnectionId: connectionID,
	}); err != nil {
		return fmt.Errorf("begin commit transaction: %w", err)
	}
	if _, err := client.Insert(ctx, insertOrder(connectionID, commitRecordName, "10.00")); err != nil {
		return fmt.Errorf("insert commit row: %w", err)
	}
	if _, err := client.Update(ctx, updateAmount(connectionID, commitRecordName, "11.25")); err != nil {
		return fmt.Errorf("update commit row: %w", err)
	}
	if _, err := client.CommitTransaction(ctx, &providerv1.CommitTransactionRequest{
		ConnectionId: connectionID,
	}); err != nil {
		return fmt.Errorf("commit local transaction: %w", err)
	}
	if err := requireOrderAmount(ctx, client, connectionID, commitRecordName, "11.25"); err != nil {
		return err
	}
	if _, err := client.Delete(ctx, deleteByName(connectionID, commitRecordName)); err != nil {
		return fmt.Errorf("delete committed row: %w", err)
	}
	if err := requireOrderCount(ctx, client, connectionID, commitRecordName, 0); err != nil {
		return err
	}

	fmt.Println("Oracle schema, query pushdown, mutations and local transactions passed")
	return nil
}

func runPrepare(ctx context.Context, client providerv1.ProviderServiceClient) error {
	connectionID, err := openConnection(ctx, client)
	if err != nil {
		return err
	}
	branch := transactionBranch()

	begun, err := client.BeginTransactionBranch(ctx, &providerv1.BeginTransactionBranchRequest{
		ConnectionId: connectionID,
		Branch:       branch,
	})
	if err != nil {
		return fmt.Errorf("begin durable branch: %w", err)
	}
	if begun.GetState() != providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_ACTIVE {
		return fmt.Errorf("begun branch state = %s", begun.GetState())
	}
	if _, err := client.Insert(ctx, insertOrder(connectionID, recoveryRecordName, "42.00")); err != nil {
		return fmt.Errorf("insert durable branch row: %w", err)
	}
	prepared, err := client.PrepareTransactionBranch(
		ctx,
		&providerv1.PrepareTransactionBranchRequest{
			ConnectionId: connectionID,
			Branch:       branch,
		})
	if err != nil {
		return fmt.Errorf("prepare durable branch: %w", err)
	}
	if prepared.GetOutcome() != providerv1.TransactionPrepareOutcome_TRANSACTION_PREPARE_OUTCOME_PREPARED {
		return fmt.Errorf("prepare outcome = %s", prepared.GetOutcome())
	}

	fmt.Println("Oracle durable branch prepared; provider may now be terminated")
	return nil
}

func runRecover(ctx context.Context, client providerv1.ProviderServiceClient) error {
	branch := transactionBranch()
	status, err := client.GetTransactionBranchStatus(
		ctx,
		&providerv1.GetTransactionBranchStatusRequest{Branch: branch})
	if err != nil {
		return fmt.Errorf("read branch status before recovery: %w", err)
	}
	if status.GetState() == providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED {
		stream, recoverErr := client.RecoverTransactionBranches(
			ctx,
			&providerv1.RecoverTransactionBranchesRequest{ResourceManagerId: resourceManagerID})
		if recoverErr != nil {
			return fmt.Errorf("start recovery scan: %w", recoverErr)
		}
		found := false
		for {
			response, receiveErr := stream.Recv()
			if errors.Is(receiveErr, io.EOF) {
				break
			}
			if receiveErr != nil {
				return fmt.Errorf("read recovery scan: %w", receiveErr)
			}
			for _, recovered := range response.GetBranches() {
				if string(recovered.GetBranch().GetBranchId()) == string(branch.GetBranchId()) {
					if recovered.GetState() != providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_PREPARED {
						return fmt.Errorf("recovered branch state = %s", recovered.GetState())
					}
					found = true
				}
			}
		}
		if !found {
			return errors.New("prepared branch was not returned by recovery scan")
		}

		committed, commitErr := client.CommitTransactionBranch(
			ctx,
			&providerv1.CommitTransactionBranchRequest{Branch: branch})
		if commitErr != nil {
			return fmt.Errorf("commit recovered branch: %w", commitErr)
		}
		if committed.GetState() != providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED {
			return fmt.Errorf("committed branch state = %s", committed.GetState())
		}
	} else if status.GetState() != providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED {
		return fmt.Errorf("branch state before recovery = %s", status.GetState())
	}

	status, err = client.GetTransactionBranchStatus(
		ctx,
		&providerv1.GetTransactionBranchStatusRequest{Branch: branch})
	if err != nil {
		return fmt.Errorf("read committed branch status: %w", err)
	}
	if status.GetState() != providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_COMMITTED {
		return fmt.Errorf("stored branch state = %s", status.GetState())
	}

	connectionID, err := openConnection(ctx, client)
	if err != nil {
		return err
	}
	defer closeConnection(context.Background(), client, connectionID)
	if err := requireOrderAmount(ctx, client, connectionID, recoveryRecordName, "42.00"); err != nil {
		return err
	}
	if _, err := client.Delete(ctx, deleteByName(connectionID, recoveryRecordName)); err != nil {
		return fmt.Errorf("delete recovered row: %w", err)
	}

	if _, err := client.ForgetTransactionBranch(
		ctx,
		&providerv1.ForgetTransactionBranchRequest{Branch: branch}); err != nil {
		return fmt.Errorf("forget recovered branch: %w", err)
	}
	status, err = client.GetTransactionBranchStatus(
		ctx,
		&providerv1.GetTransactionBranchStatusRequest{Branch: branch})
	if err != nil {
		return fmt.Errorf("read forgotten branch status: %w", err)
	}
	if status.GetState() != providerv1.TransactionBranchState_TRANSACTION_BRANCH_STATE_NOT_FOUND {
		return fmt.Errorf("forgotten branch state = %s", status.GetState())
	}

	fmt.Println("Oracle prepared branch recovered and committed after provider restart")
	return nil
}

func requireTables(metadata *providerv1.SchemaMetadata, names ...string) error {
	found := make(map[string]bool)
	for _, table := range metadata.GetTables() {
		if table.GetNamespace() == "KUBLING" {
			found[table.GetName()] = true
		}
	}
	for _, name := range names {
		if !found[name] {
			return fmt.Errorf("schema does not expose KUBLING.%s", name)
		}
	}
	return nil
}

func requireWritableTables(metadata *providerv1.SchemaMetadata, names ...string) error {
	tables := make(map[string]*providerv1.TableMetadata)
	for _, table := range metadata.GetTables() {
		if table.GetNamespace() == "KUBLING" {
			tables[table.GetName()] = table
		}
	}
	for _, name := range names {
		table := tables[name]
		if table == nil {
			return fmt.Errorf("schema does not expose KUBLING.%s", name)
		}
		if !table.GetUpdatable() {
			return fmt.Errorf("schema reports KUBLING.%s as read-only", name)
		}
		writableColumn := false
		for _, column := range table.GetColumns() {
			if column.GetUpdatable() {
				writableColumn = true
				break
			}
		}
		if !writableColumn {
			return fmt.Errorf("schema reports no writable columns for KUBLING.%s", name)
		}
	}
	return nil
}

func requireReadOnlyTables(metadata *providerv1.SchemaMetadata, names ...string) error {
	tables := make(map[string]*providerv1.TableMetadata)
	for _, table := range metadata.GetTables() {
		if table.GetNamespace() == "KUBLING" {
			tables[table.GetName()] = table
		}
	}
	for _, name := range names {
		table := tables[name]
		if table == nil {
			return fmt.Errorf("schema does not expose KUBLING.%s", name)
		}
		if table.GetUpdatable() {
			return fmt.Errorf("schema reports KUBLING.%s as writable", name)
		}
	}
	return nil
}

func openConnection(
	ctx context.Context,
	client providerv1.ProviderServiceClient,
) (string, error) {
	opened, err := client.OpenConnection(ctx, &providerv1.OpenConnectionRequest{})
	if err != nil {
		return "", fmt.Errorf("open logical connection: %w", err)
	}
	if opened.GetConnectionId() == "" {
		return "", errors.New("provider returned an empty logical connection ID")
	}
	return opened.GetConnectionId(), nil
}

func closeConnection(
	ctx context.Context,
	client providerv1.ProviderServiceClient,
	connectionID string,
) {
	closeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, _ = client.CloseConnection(closeCtx, &providerv1.CloseConnectionRequest{
		ConnectionId: connectionID,
	})
}

func orderedPage(connectionID string) *providerv1.QueryRequest {
	limit := uint64(1)
	offset := uint64(1)
	return &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       orderEntity(),
		Projections: []*providerv1.Projection{
			projection(field("select"), "select"),
			projection(field("Amount"), "Amount"),
		},
		Filter: comparison(
			providerv1.ComparisonOperator_COMPARISON_OPERATOR_GREATER_THAN,
			field("Amount"),
			decimalLiteral("0")),
		OrderBy: []*providerv1.OrderBy{{
			Expression:   field("id"),
			Direction:    providerv1.SortDirection_SORT_DIRECTION_DESCENDING,
			NullOrdering: providerv1.NullOrdering_NULL_ORDERING_LAST,
		}},
		Limit:         &limit,
		Offset:        &offset,
		AcceptOutcome: true,
	}
}

func requireOrderAmount(
	ctx context.Context,
	client providerv1.ProviderServiceClient,
	connectionID string,
	name string,
	want string,
) error {
	rows, err := query(ctx, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       orderEntity(),
		Projections: []*providerv1.Projection{
			projection(field("Amount"), "Amount"),
		},
		Filter: comparison(
			providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			field("select"),
			stringLiteral(name)),
		AcceptOutcome: true,
	})
	if err != nil {
		return fmt.Errorf("query order %q: %w", name, err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		return fmt.Errorf("order %q amount rows = %v, want %s", name, rows, want)
	}
	got := rows[0][0].GetBigdecimalValue()
	if !equalDecimal(got, want) {
		return fmt.Errorf("order %q amount = %s, want %s", name, got, want)
	}
	return nil
}

func equalDecimal(left, right string) bool {
	leftValue, leftOK := new(big.Rat).SetString(left)
	rightValue, rightOK := new(big.Rat).SetString(right)
	return leftOK && rightOK && leftValue.Cmp(rightValue) == 0
}

func requireOrderCount(
	ctx context.Context,
	client providerv1.ProviderServiceClient,
	connectionID string,
	name string,
	want int64,
) error {
	filter := comparison(
		providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
		field("select"),
		stringLiteral(name))
	count, err := countOrders(ctx, client, connectionID, filter)
	if err != nil {
		return err
	}
	if count != want {
		return fmt.Errorf("order %q count = %d, want %d", name, count, want)
	}
	return nil
}

func countOrders(
	ctx context.Context,
	client providerv1.ProviderServiceClient,
	connectionID string,
	filter *providerv1.Expression,
) (int64, error) {
	return countRows(ctx, client, connectionID, orderEntity(), filter)
}

func countRows(
	ctx context.Context,
	client providerv1.ProviderServiceClient,
	connectionID string,
	entity *providerv1.EntityReference,
	filter *providerv1.Expression,
) (int64, error) {
	rows, err := query(ctx, client, &providerv1.QueryRequest{
		ConnectionId: connectionID,
		Entity:       entity,
		Projections: []*providerv1.Projection{projection(
			aggregate(
				providerv1.AggregateFunction_AGGREGATE_FUNCTION_COUNT_STAR,
				kublingv1.ValueType_VALUE_TYPE_LONG),
			"count")},
		Filter:        filter,
		AcceptOutcome: true,
	})
	if err != nil {
		return 0, err
	}
	if len(rows) != 1 || len(rows[0]) != 1 {
		return 0, fmt.Errorf("count returned unexpected rows: %v", rows)
	}
	return rows[0][0].GetLongValue(), nil
}

func query(
	ctx context.Context,
	client providerv1.ProviderServiceClient,
	request *providerv1.QueryRequest,
) ([][]*kublingv1.Value, error) {
	stream, err := client.Query(ctx, request)
	if err != nil {
		return nil, err
	}
	var rows [][]*kublingv1.Value
	complete := false
	for {
		response, receiveErr := stream.Recv()
		if errors.Is(receiveErr, io.EOF) {
			break
		}
		if receiveErr != nil {
			return nil, receiveErr
		}
		if response.GetBatch() != nil {
			for _, tuple := range response.GetBatch().GetTuples() {
				rows = append(rows, tuple.GetValues())
			}
		}
		if response.GetOutcome() != nil {
			complete = response.GetOutcome().GetCompletion() ==
				providerv1.QueryCompletion_QUERY_COMPLETION_COMPLETE
		}
	}
	if request.GetAcceptOutcome() && !complete {
		return nil, errors.New("query did not return a complete terminal outcome")
	}
	return rows, nil
}

func insertOrder(connectionID, name, amount string) *providerv1.InsertRequest {
	return &providerv1.InsertRequest{
		ConnectionId: connectionID,
		Entity:       orderEntity(),
		Rows: &providerv1.TupleBatch{
			Fields: []*providerv1.Field{
				tupleField("select", kublingv1.ValueType_VALUE_TYPE_STRING),
				tupleField("Amount", kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL),
			},
			Tuples: []*providerv1.Tuple{{Values: []*kublingv1.Value{
				stringValue(name),
				decimalValue(amount),
			}}},
		},
	}
}

func updateAmount(connectionID, name, amount string) *providerv1.UpdateRequest {
	return &providerv1.UpdateRequest{
		ConnectionId: connectionID,
		Entity:       orderEntity(),
		Assignments: []*providerv1.Assignment{{
			Field: "Amount",
			Value: decimalLiteral(amount),
		}},
		Filter: comparison(
			providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			field("select"),
			stringLiteral(name)),
	}
}

func deleteByName(connectionID, name string) *providerv1.DeleteRequest {
	return &providerv1.DeleteRequest{
		ConnectionId: connectionID,
		Entity:       orderEntity(),
		Filter: comparison(
			providerv1.ComparisonOperator_COMPARISON_OPERATOR_EQUAL,
			field("select"),
			stringLiteral(name)),
	}
}

func orderEntity() *providerv1.EntityReference {
	return &providerv1.EntityReference{Namespace: "KUBLING", Name: "Order"}
}

func salesOrderDetailEntity() *providerv1.EntityReference {
	return &providerv1.EntityReference{Namespace: "KUBLING", Name: "SALES_ORDER_DETAIL"}
}

func transactionBranch() *providerv1.TransactionBranchId {
	return &providerv1.TransactionBranchId{
		ResourceManagerId: resourceManagerID,
		BranchId:          branchID,
	}
}

func tupleField(name string, valueType kublingv1.ValueType) *providerv1.Field {
	return &providerv1.Field{
		Name:           name,
		Type:           valueType,
		TypeDescriptor: &kublingv1.TypeDescriptor{Type: valueType},
	}
}

func projection(expression *providerv1.Expression, name string) *providerv1.Projection {
	return &providerv1.Projection{Expression: expression, OutputName: name}
}

func field(name string) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Field{
		Field: &providerv1.FieldReference{Name: name},
	}}
}

func aggregate(
	function providerv1.AggregateFunction,
	resultType kublingv1.ValueType,
) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Aggregate{
		Aggregate: &providerv1.AggregateCall{
			Function:   function,
			ResultType: &kublingv1.TypeDescriptor{Type: resultType},
		},
	}}
}

func comparison(
	operator providerv1.ComparisonOperator,
	left *providerv1.Expression,
	right *providerv1.Expression,
) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Comparison{
		Comparison: &providerv1.ComparisonExpression{
			Operator: operator,
			Left:     left,
			Right:    right,
		},
	}}
}

func stringLiteral(value string) *providerv1.Expression {
	return literal(stringValue(value), kublingv1.ValueType_VALUE_TYPE_STRING)
}

func decimalLiteral(value string) *providerv1.Expression {
	return literal(decimalValue(value), kublingv1.ValueType_VALUE_TYPE_BIGDECIMAL)
}

func literal(value *kublingv1.Value, valueType kublingv1.ValueType) *providerv1.Expression {
	return &providerv1.Expression{Kind: &providerv1.Expression_Literal{
		Literal: &providerv1.Literal{
			Value:        value,
			DeclaredType: &kublingv1.TypeDescriptor{Type: valueType},
		},
	}}
}

func stringValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_StringValue{StringValue: value}}
}

func decimalValue(value string) *kublingv1.Value {
	return &kublingv1.Value{Kind: &kublingv1.Value_BigdecimalValue{BigdecimalValue: value}}
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
