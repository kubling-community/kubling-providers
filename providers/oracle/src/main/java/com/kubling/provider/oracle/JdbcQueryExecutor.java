package com.kubling.provider.oracle;

import com.kubling.provider.grpc.Field;
import com.kubling.provider.grpc.QueryCompletion;
import com.kubling.provider.grpc.QueryOutcome;
import com.kubling.provider.grpc.QueryRequest;
import com.kubling.provider.grpc.QueryResponse;
import com.kubling.provider.grpc.Tuple;
import com.kubling.provider.grpc.TupleBatch;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JdbcQueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcQueryExecutor.class);

    void execute(
            Connection connection,
            QueryRequest request,
            QueryPlan plan,
            StreamObserver<QueryResponse> observer) throws SQLException {
        Context context = Context.current();
        checkCancelled(context);
        try (PreparedStatement statement = connection.prepareStatement(plan.sql())) {
            statement.setFetchSize(plan.batchSize());
            for (int index = 0; index < plan.parameters().size(); index++) {
                JdbcValueCodec.bind(statement, index + 1, plan.parameters().get(index));
            }

            Context.CancellationListener cancellationListener = ignored -> cancel(statement);
            context.addListener(cancellationListener, Runnable::run);
            try (ResultSet resultSet = statement.executeQuery()) {
                List<JdbcValueCodec.ResultColumn> columns = resultColumns(
                        request,
                        resultSet.getMetaData());
                streamRows(context, request, plan, resultSet, columns, observer);
            } finally {
                context.removeListener(cancellationListener);
            }
        }
    }

    private static List<JdbcValueCodec.ResultColumn> resultColumns(
            QueryRequest request,
            ResultSetMetaData metadata) throws SQLException {
        if (metadata.getColumnCount() != request.getProjectionsCount()) {
            throw new SQLException("Oracle result column count does not match the query projection");
        }
        List<JdbcValueCodec.ResultColumn> columns = new ArrayList<>();
        for (int index = 0; index < request.getProjectionsCount(); index++) {
            columns.add(JdbcValueCodec.resultColumn(
                    request.getProjections(index),
                    metadata,
                    index + 1));
        }
        return List.copyOf(columns);
    }

    private static void streamRows(
            Context context,
            QueryRequest request,
            QueryPlan plan,
            ResultSet resultSet,
            List<JdbcValueCodec.ResultColumn> columns,
            StreamObserver<QueryResponse> observer) throws SQLException {
        List<Field> fields = columns.stream()
                .map(JdbcValueCodec.ResultColumn::field)
                .toList();
        TupleBatch.Builder batch = newBatch(fields);
        while (resultSet.next()) {
            checkCancelled(context);
            Tuple.Builder tuple = Tuple.newBuilder();
            for (int index = 0; index < columns.size(); index++) {
                tuple.addValues(JdbcValueCodec.read(
                        resultSet,
                        index + 1,
                        columns.get(index).type()));
            }
            batch.addTuples(tuple);
            if (batch.getTuplesCount() == plan.batchSize()) {
                observer.onNext(QueryResponse.newBuilder().setBatch(batch).build());
                batch = newBatch(fields);
            }
        }
        if (batch.getTuplesCount() > 0) {
            observer.onNext(QueryResponse.newBuilder().setBatch(batch).build());
        }
        if (request.getAcceptOutcome()) {
            observer.onNext(QueryResponse.newBuilder()
                    .setOutcome(QueryOutcome.newBuilder()
                            .setCompletion(QueryCompletion.QUERY_COMPLETION_COMPLETE))
                    .build());
        }
    }

    private static TupleBatch.Builder newBatch(List<Field> fields) {
        return TupleBatch.newBuilder().addAllFields(fields);
    }

    private static void checkCancelled(Context context) {
        if (context.isCancelled()) {
            throw Status.CANCELLED
                    .withDescription("Oracle query was cancelled")
                    .asRuntimeException();
        }
    }

    private static void cancel(PreparedStatement statement) {
        try {
            statement.cancel();
        } catch (SQLException exception) {
            LOGGER.debug(
                    "Oracle statement cancellation failed ({})",
                    exception.getClass().getSimpleName());
        }
    }
}
