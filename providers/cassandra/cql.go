package cassandra

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/cassandra-gocql-driver/v2"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type resolvedEntity struct {
	namespace string
	config    DataSourceConfig
	session   driverSession
	table     *gocql.TableMetadata
}

func (c *Connection) resolveEntity(
	ctx context.Context,
	reference *providerv1.EntityReference,
) (*resolvedEntity, error) {
	if err := c.ensureOpen(); err != nil {
		return nil, err
	}
	if reference == nil || strings.TrimSpace(reference.GetName()) == "" {
		return nil, status.Error(codes.InvalidArgument, "entity is required")
	}
	namespace := reference.GetNamespace()
	if namespace == "" {
		return nil, status.Error(
			codes.InvalidArgument,
			"Cassandra entities require a namespace",
		)
	}

	config, exists := c.provider.config.DataSources[namespace]
	if !exists {
		return nil, status.Errorf(
			codes.NotFound,
			"Cassandra namespace %q was not found",
			namespace,
		)
	}

	session, err := c.session(ctx, namespace, config)
	if err != nil {
		return nil, err
	}

	keyspace, err := session.KeyspaceMetadata(config.Keyspace)
	if err != nil {
		return nil, operationError("discover Cassandra entity", err)
	}
	if keyspace == nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cassandra returned nil metadata for namespace %q",
			namespace,
		)
	}

	table := findTable(keyspace, reference.GetName())
	if table == nil {
		return nil, status.Errorf(
			codes.NotFound,
			"entity %q was not found in namespace %q",
			reference.GetName(),
			namespace,
		)
	}

	return &resolvedEntity{
		namespace: namespace,
		config:    config,
		session:   session,
		table:     table,
	}, nil
}

func (*resolvedEntity) Close() {}

func (e *resolvedEntity) qualifiedTable() string {
	return quoteIdentifier(e.config.Keyspace) + "." + quoteIdentifier(e.table.Name)
}

func findTable(
	keyspace *gocql.KeyspaceMetadata,
	name string,
) *gocql.TableMetadata {
	if table := keyspace.Tables[name]; table != nil {
		return table
	}
	for tableName, table := range keyspace.Tables {
		if strings.EqualFold(tableName, name) {
			return table
		}
	}

	return nil
}

func findColumn(
	table *gocql.TableMetadata,
	name string,
) *gocql.ColumnMetadata {
	if column := table.Columns[name]; column != nil {
		return column
	}
	for columnName, column := range table.Columns {
		if strings.EqualFold(columnName, name) {
			return column
		}
	}

	return nil
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

func operationError(operation string, err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}

	var invalid *gocql.RequestErrInvalid
	var syntax *gocql.RequestErrSyntax
	var unauthorized *gocql.RequestErrUnauthorized
	switch {
	case errors.As(err, &invalid), errors.As(err, &syntax):
		return newStatusError(
			codes.InvalidArgument,
			fmt.Sprintf("%s: %v", operation, err),
			err,
		)
	case errors.As(err, &unauthorized):
		return newStatusError(
			codes.PermissionDenied,
			fmt.Sprintf("%s: %v", operation, err),
			err,
		)
	default:
		return newStatusError(
			codes.Unavailable,
			fmt.Sprintf("%s: %v", operation, err),
			err,
		)
	}
}
