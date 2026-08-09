package cassandra

import (
	"context"

	"github.com/apache/cassandra-gocql-driver/v2"
)

type driverSession interface {
	Close()
	KeyspaceMetadata(string) (*gocql.KeyspaceMetadata, error)
	Query(context.Context, string, []any, int) driverIterator
	Exec(context.Context, string, []any) error
}

type driverIterator interface {
	Columns() []gocql.ColumnInfo
	MapScan(map[string]any) bool
	Close() error
}

type sessionFactory func(
	context.Context,
	DataSourceConfig,
) (driverSession, error)

type gocqlSession struct {
	session *gocql.Session
}

func (s *gocqlSession) Close() {
	s.session.Close()
}

func (s *gocqlSession) KeyspaceMetadata(
	keyspace string,
) (*gocql.KeyspaceMetadata, error) {
	return s.session.KeyspaceMetadata(keyspace)
}

func (s *gocqlSession) Query(
	ctx context.Context,
	statement string,
	values []any,
	pageSize int,
) driverIterator {
	query := s.session.Query(statement, values...).WithContext(ctx)
	if pageSize > 0 {
		query.PageSize(pageSize)
	}

	return query.Iter()
}

func (s *gocqlSession) Exec(
	ctx context.Context,
	statement string,
	values []any,
) error {
	return s.session.Query(statement, values...).WithContext(ctx).Exec()
}

func createSession(
	ctx context.Context,
	config DataSourceConfig,
) (driverSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cluster, err := buildClusterConfig(config)
	if err != nil {
		return nil, err
	}

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		session.Close()
		return nil, err
	}

	return &gocqlSession{session: session}, nil
}

var _ driverSession = (*gocqlSession)(nil)
