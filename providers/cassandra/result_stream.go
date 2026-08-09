package cassandra

import (
	"context"
	"errors"
	"io"
	"sync"

	kublingv1 "github.com/kubling-community/kubling-grpc/sdk-go/kubling/v1"
	providerv1 "github.com/kubling-community/kubling-providers/sdk-go/kubling/provider/v1"
	providersdk "github.com/kubling-community/kubling-providers/sdk-go/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type cassandraResultStream struct {
	mu          sync.Mutex
	entity      *resolvedEntity
	iterator    driverIterator
	projections []projectionPlan
	fields      []*providerv1.Field
	batchSize   int
	done        bool
	closed      bool
	iterClosed  bool
	terminalErr error
}

func newCassandraResultStream(
	entity *resolvedEntity,
	iterator driverIterator,
	projections []projectionPlan,
	batchSize int,
) providersdk.ResultStream {
	fields := make([]*providerv1.Field, 0, len(projections))
	for _, projection := range projections {
		fields = append(fields, &providerv1.Field{
			Name: projection.outputName,
			Type: logicalValueType(projection.column.Type),
		})
	}

	return &cassandraResultStream{
		entity:      entity,
		iterator:    iterator,
		projections: projections,
		fields:      fields,
		batchSize:   batchSize,
	}
}

func (s *cassandraResultStream) Next(
	ctx context.Context,
) (*providerv1.TupleBatch, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, io.EOF
	}
	if s.done {
		if s.terminalErr != nil {
			return nil, s.terminalErr
		}
		return nil, io.EOF
	}

	tuples := make([]*providerv1.Tuple, 0, s.batchSize)
	for len(tuples) < s.batchSize {
		row := make(map[string]any, len(s.projections))
		if !s.iterator.MapScan(row) {
			closeErr := s.closeIteratorLocked()
			s.done = true
			if closeErr != nil {
				s.terminalErr = operationError("execute Cassandra query", closeErr)
			}
			if len(tuples) == 0 {
				if s.terminalErr != nil {
					return nil, s.terminalErr
				}
				return nil, io.EOF
			}
			break
		}

		values := make([]*kublingv1.Value, 0, len(s.projections))
		for _, projection := range s.projections {
			value, err := nativeToValue(
				projection.column.Type,
				row[projection.column.Name],
			)
			if err != nil {
				closeErr := s.closeIteratorLocked()
				s.done = true
				return nil, errors.Join(
					status.Errorf(
						codes.Internal,
						"convert Cassandra column %q: %v",
						projection.column.Name,
						err,
					),
					closeErr,
				)
			}
			values = append(values, value)
		}
		tuples = append(tuples, &providerv1.Tuple{Values: values})
	}

	return &providerv1.TupleBatch{
		Fields: s.fields,
		Tuples: tuples,
	}, nil
}

func (s *cassandraResultStream) Close() error {
	s.mu.Lock()
	if s.closed {
		err := s.terminalErr
		s.mu.Unlock()
		return err
	}
	s.closed = true
	closeErr := s.closeIteratorLocked()
	if s.terminalErr == nil && closeErr != nil {
		s.terminalErr = operationError("execute Cassandra query", closeErr)
	}
	err := s.terminalErr
	entity := s.entity
	s.mu.Unlock()

	entity.Close()
	return err
}

func (s *cassandraResultStream) closeIteratorLocked() error {
	if s.iterClosed {
		return nil
	}
	s.iterClosed = true
	return s.iterator.Close()
}

var _ providersdk.ResultStream = (*cassandraResultStream)(nil)
