package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kubling-community/kubling-grpc/sdk-go/client"
	"github.com/kubling-community/kubling-grpc/sdk-go/exec"
	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	"github.com/kubling-community/kubling-grpc/sdk-go/result"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: provider-test <smoke|canonical|query|exec>")
	}

	switch args[0] {
	case "smoke":
		return runSmoke()
	case "canonical":
		return runCanonical()
	case "query":
		return runQuery(args[1:])
	case "exec":
		return runExec(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runSmoke() error {
	cli, err := waitForClient()
	if err != nil {
		return err
	}
	defer cli.Close()

	columns, rows, err := query(cli, `
		SELECT Name AS table_name, SchemaName AS schema_name
		FROM SYS.Tables
		WHERE SchemaName = 'provider' AND Type = 'Table'
		ORDER BY Name
	`)
	if err != nil {
		return fmt.Errorf("query imported tables: %w", err)
	}
	if len(rows) == 0 {
		return errors.New("Kubling imported no tables for the provider data source")
	}

	fmt.Printf("Kubling imported %d provider table(s):\n", len(rows))
	printRows(columns, rows)
	return nil
}

func runCanonical() (returnErr error) {
	cli, err := waitForClient()
	if err != nil {
		return err
	}
	defer cli.Close()

	checks := []struct {
		table  string
		column string
		value  string
	}{
		{table: "PROJECT", column: "id", value: "project-1"},
		{table: "PROJECT", column: "id", value: "project-2"},
		{table: "TASK", column: "id", value: "task-1"},
		{table: "TASK", column: "id", value: "task-2"},
		{table: "TASK", column: "id", value: "task-3"},
		{table: "AUDIT_EVENT", column: "id", value: "1001"},
		{table: "AUDIT_EVENT", column: "id", value: "1002"},
		{table: "TYPE_SAMPLE", column: "sample_id", value: "canonical"},
	}

	for _, check := range checks {
		if err := assertRowCount(cli, check.table, check.column, check.value, 1); err != nil {
			return err
		}
	}

	fmt.Println("Canonical fixture records are available through Kubling")

	mutations, err := strconv.ParseBool(env("KUBLING_TEST_MUTATIONS", "true"))
	if err != nil {
		return fmt.Errorf("parse KUBLING_TEST_MUTATIONS: %w", err)
	}
	if !mutations {
		fmt.Println("Canonical TASK mutation checks skipped")
		return nil
	}

	taskID := fmt.Sprintf("kubling-profile-%d", time.Now().UnixNano())
	cleanupNeeded := true
	defer func() {
		if !cleanupNeeded {
			return
		}
		_, cleanupErr := exec.Exec(cli, fmt.Sprintf("DELETE FROM provider.TASK WHERE id = '%s'", taskID))
		if returnErr == nil && cleanupErr != nil {
			returnErr = fmt.Errorf("clean up canonical task: %w", cleanupErr)
		}
	}()

	if _, err := exec.Exec(cli, fmt.Sprintf(`
		INSERT INTO provider.TASK (id, project_id, title, completed, priority)
		VALUES ('%s', 'project-1', 'Canonical profile insert', false, 10)
	`, taskID)); err != nil {
		return fmt.Errorf("insert canonical task: %w", err)
	}
	if err := assertRowCount(cli, "TASK", "id", taskID, 1); err != nil {
		return err
	}

	if _, err := exec.Exec(cli, fmt.Sprintf(`
		UPDATE provider.TASK
		SET title = 'Canonical profile update'
		WHERE id = '%s'
	`, taskID)); err != nil {
		return fmt.Errorf("update canonical task: %w", err)
	}

	_, rows, err := query(cli, fmt.Sprintf(
		"SELECT title FROM provider.TASK WHERE id = '%s'",
		taskID,
	))
	if err != nil {
		return fmt.Errorf("read updated canonical task: %w", err)
	}
	if len(rows) != 1 || len(rows[0]) != 1 || rows[0][0] != "Canonical profile update" {
		return fmt.Errorf("TASK update returned unexpected rows: %v", rows)
	}

	if _, err := exec.Exec(cli, fmt.Sprintf("DELETE FROM provider.TASK WHERE id = '%s'", taskID)); err != nil {
		return fmt.Errorf("delete canonical task: %w", err)
	}
	if err := assertRowCount(cli, "TASK", "id", taskID, 0); err != nil {
		return err
	}
	cleanupNeeded = false

	fmt.Println("Canonical TASK mutations passed through Kubling")
	return nil
}

func runQuery(args []string) error {
	flags := flag.NewFlagSet("query", flag.ContinueOnError)
	sql := flags.String("sql", "", "SQL query to execute")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*sql) == "" {
		return errors.New("query requires -sql")
	}

	cli, err := waitForClient()
	if err != nil {
		return err
	}
	defer cli.Close()

	columns, rows, err := query(cli, *sql)
	if err != nil {
		return err
	}
	printRows(columns, rows)
	return nil
}

func runExec(args []string) error {
	flags := flag.NewFlagSet("exec", flag.ContinueOnError)
	sql := flags.String("sql", "", "SQL statement to execute")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*sql) == "" {
		return errors.New("exec requires -sql")
	}

	cli, err := waitForClient()
	if err != nil {
		return err
	}
	defer cli.Close()

	execResult, err := exec.Exec(cli, *sql)
	if err != nil {
		return err
	}
	fmt.Printf("affected rows: %d\n", execResult.AffectedRows())
	return nil
}

func waitForClient() (*client.Client, error) {
	attempts, err := strconv.Atoi(env("KUBLING_TEST_WAIT_ATTEMPTS", "30"))
	if err != nil || attempts < 1 {
		return nil, fmt.Errorf("invalid KUBLING_TEST_WAIT_ATTEMPTS %q", os.Getenv("KUBLING_TEST_WAIT_ATTEMPTS"))
	}
	waitSeconds, err := strconv.Atoi(env("KUBLING_TEST_WAIT_SECONDS", "2"))
	if err != nil || waitSeconds < 0 {
		return nil, fmt.Errorf("invalid KUBLING_TEST_WAIT_SECONDS %q", os.Getenv("KUBLING_TEST_WAIT_SECONDS"))
	}

	options := client.Options{
		Address:  env("KUBLING_TEST_GRPC_ADDRESS", "localhost:50061"),
		Username: env("KUBLING_TEST_USER", "sa"),
		Password: env("KUBLING_TEST_PASSWORD", "sa"),
		VDBName:  env("KUBLING_TEST_VDB", "ProviderTestVDB"),
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		cli, err := client.NewClient(options)
		if err == nil {
			return cli, nil
		}
		lastErr = err
		if attempt < attempts {
			time.Sleep(time.Duration(waitSeconds) * time.Second)
		}
	}

	return nil, fmt.Errorf("Kubling did not become available after %d attempts: %w", attempts, lastErr)
}

func assertRowCount(cli *client.Client, table, column, value string, expected int) error {
	_, rows, err := query(cli, fmt.Sprintf(
		"SELECT %s FROM provider.%s WHERE %s = '%s'",
		column,
		table,
		column,
		value,
	))
	if err != nil {
		return fmt.Errorf("query %s.%s=%s: %w", table, column, value, err)
	}
	if len(rows) != expected {
		return fmt.Errorf("%s.%s=%s: expected %d row(s), got %d", table, column, value, expected, len(rows))
	}
	return nil
}

func query(cli *client.Client, sql string) ([]string, [][]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream, err := cli.Query().Query(ctx, &kublingv1.QueryRequest{
		ExpiringToken: cli.Token(),
		Sql:           sql,
	})
	if err != nil {
		return nil, nil, err
	}

	var columns []string
	var rows [][]any
	for {
		batch, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return columns, rows, nil
		}
		if err != nil {
			return nil, nil, err
		}
		if columns == nil {
			columns = make([]string, len(batch.GetColumns()))
			for index, column := range batch.GetColumns() {
				columns[index] = column.GetName()
			}
		}
		for _, row := range batch.GetRows() {
			values := make([]any, len(row.GetValues()))
			for index, value := range row.GetValues() {
				values[index], err = result.DecodeValue(value)
				if err != nil {
					return nil, nil, fmt.Errorf("decode column %d: %w", index, err)
				}
			}
			rows = append(rows, values)
		}
	}
}

func printRows(columns []string, rows [][]any) {
	fmt.Println(strings.Join(columns, "\t"))
	for _, row := range rows {
		values := make([]string, len(row))
		for index, value := range row {
			if value == nil {
				values[index] = "NULL"
			} else {
				values[index] = fmt.Sprint(value)
			}
		}
		fmt.Println(strings.Join(values, "\t"))
	}
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
