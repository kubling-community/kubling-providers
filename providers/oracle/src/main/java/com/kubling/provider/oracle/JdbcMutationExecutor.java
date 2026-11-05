package com.kubling.provider.oracle;

import com.kubling.provider.grpc.Literal;
import io.grpc.Context;
import io.grpc.Status;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.OptionalLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class JdbcMutationExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcMutationExecutor.class);

    OptionalLong insert(Connection connection, InsertPlan plan) throws SQLException {
        Context context = Context.current();
        checkCancelled(context);
        try (PreparedStatement statement = connection.prepareStatement(plan.sql())) {
            Context.CancellationListener listener = ignored -> cancel(statement);
            context.addListener(listener, Runnable::run);
            try {
                for (List<Literal> row : plan.rows()) {
                    checkCancelled(context);
                    statement.clearParameters();
                    bind(statement, row);
                    statement.addBatch();
                }
                checkCancelled(context);
                long[] counts = statement.executeLargeBatch();
                if (counts.length != plan.rows().size()) {
                    throw new SQLException("Oracle insert batch returned an invalid result count");
                }
                return affectedRows(counts);
            } finally {
                context.removeListener(listener);
            }
        }
    }

    long execute(Connection connection, MutationPlan plan) throws SQLException {
        Context context = Context.current();
        checkCancelled(context);
        try (PreparedStatement statement = connection.prepareStatement(plan.sql())) {
            bind(statement, plan.parameters());
            Context.CancellationListener listener = ignored -> cancel(statement);
            context.addListener(listener, Runnable::run);
            try {
                checkCancelled(context);
                return statement.executeLargeUpdate();
            } finally {
                context.removeListener(listener);
            }
        }
    }

    private static void bind(PreparedStatement statement, List<Literal> parameters)
            throws SQLException {
        for (int index = 0; index < parameters.size(); index++) {
            JdbcValueCodec.bind(statement, index + 1, parameters.get(index));
        }
    }

    private static OptionalLong affectedRows(long[] counts) throws SQLException {
        long total = 0;
        boolean exact = true;
        try {
            for (long count : counts) {
                if (count == Statement.EXECUTE_FAILED) {
                    throw new SQLException("Oracle insert batch reported a failed command");
                }
                if (count == Statement.SUCCESS_NO_INFO) {
                    exact = false;
                } else if (count < 0) {
                    throw new SQLException("Oracle insert batch returned an invalid update count");
                } else {
                    total = Math.addExact(total, count);
                }
            }
        } catch (ArithmeticException exception) {
            throw new SQLException("Oracle affected-row count overflowed", exception);
        }
        return exact ? OptionalLong.of(total) : OptionalLong.empty();
    }

    private static void checkCancelled(Context context) {
        if (context.isCancelled()) {
            throw Status.CANCELLED
                    .withDescription("Oracle mutation was cancelled")
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
