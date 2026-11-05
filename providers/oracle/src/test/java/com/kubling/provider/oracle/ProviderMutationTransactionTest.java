package com.kubling.provider.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kubling.provider.grpc.Assignment;
import com.kubling.provider.grpc.BeginTransactionRequest;
import com.kubling.provider.grpc.CloseConnectionRequest;
import com.kubling.provider.grpc.CommitTransactionRequest;
import com.kubling.provider.grpc.ComparisonExpression;
import com.kubling.provider.grpc.ComparisonOperator;
import com.kubling.provider.grpc.DeleteRequest;
import com.kubling.provider.grpc.EntityReference;
import com.kubling.provider.grpc.Expression;
import com.kubling.provider.grpc.Field;
import com.kubling.provider.grpc.FieldReference;
import com.kubling.provider.grpc.InsertRequest;
import com.kubling.provider.grpc.InsertResponse;
import com.kubling.provider.grpc.IsInTransactionRequest;
import com.kubling.provider.grpc.Literal;
import com.kubling.provider.grpc.OpenConnectionRequest;
import com.kubling.provider.grpc.OpenConnectionResponse;
import com.kubling.provider.grpc.ProviderServiceGrpc;
import com.kubling.provider.grpc.RollbackTransactionRequest;
import com.kubling.provider.grpc.SchemaMetadata;
import com.kubling.provider.grpc.Tuple;
import com.kubling.provider.grpc.TupleBatch;
import com.kubling.provider.grpc.UpdateRequest;
import com.kubling.transport.grpc.TypeDescriptor;
import com.kubling.transport.grpc.Value;
import com.kubling.transport.grpc.ValueType;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class ProviderMutationTransactionTest {
    @Test
    void executesMutationsInsideNativeLocalTransactionsOverGrpc() throws Exception {
        JdbcFixture fixture = new JdbcFixture();
        ManagedChannel channel = null;
        try (ProviderServer server = server(fixture)) {
            server.start();
            channel = channel(server);
            ProviderServiceGrpc.ProviderServiceBlockingStub stub = stub(channel);
            String connectionId = open(stub);

            stub.beginTransaction(BeginTransactionRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .build());
            assertTrue(stub.isInTransaction(IsInTransactionRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .build()).getActive());

            InsertResponse inserted = stub.insert(insert(connectionId));
            assertTrue(inserted.hasAffectedRows());
            assertEquals(2, inserted.getAffectedRows());

            assertEquals(3, stub.update(update(connectionId)).getAffectedRows());
            assertEquals(2, stub.delete(delete(connectionId)).getAffectedRows());

            stub.commitTransaction(CommitTransactionRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .build());
            assertFalse(stub.isInTransaction(IsInTransactionRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .build()).getActive());

            stub.beginTransaction(BeginTransactionRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .build());
            stub.rollbackTransaction(RollbackTransactionRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .build());
            stub.closeConnection(CloseConnectionRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .build());
        } finally {
            close(channel);
        }

        assertTrue(fixture.autoCommit.get());
        assertEquals(1, fixture.commits.get());
        assertEquals(1, fixture.rollbacks.get());
        assertTrue(fixture.closed.get());
        assertEquals(3, fixture.executions.size());

        Execution insert = fixture.executions.get(0);
        assertEquals(
                "INSERT INTO \"KUBLING\".\"Order\" (\"id\", \"select\") VALUES (?, ?)",
                insert.sql());
        assertEquals(2, insert.parameters().size());
        assertEquals(7L, insert.parameters().get(0).get(1));
        assertEquals("second", insert.parameters().get(1).get(2));

        Execution update = fixture.executions.get(1);
        assertEquals(
                "UPDATE \"KUBLING\".\"Order\" SET \"Amount\" = ? "
                        + "WHERE (\"id\" = ?)",
                update.sql());
        assertEquals(new BigDecimal("19.95"), update.parameters().get(0).get(1));
        assertEquals(7L, update.parameters().get(0).get(2));

        Execution delete = fixture.executions.get(2);
        assertEquals(
                "DELETE FROM \"KUBLING\".\"Order\" WHERE (\"select\" = ?)",
                delete.sql());
        assertEquals("second", delete.parameters().get(0).get(1));
    }

    @Test
    void rejectsInvalidTransactionTransitionsAndRollsBackOnClose() throws Exception {
        JdbcFixture fixture = new JdbcFixture();
        ManagedChannel channel = null;
        try (ProviderServer server = server(fixture)) {
            server.start();
            channel = channel(server);
            ProviderServiceGrpc.ProviderServiceBlockingStub stub = stub(channel);
            String connectionId = open(stub);

            StatusRuntimeException noTransaction = assertThrows(
                    StatusRuntimeException.class,
                    () -> stub.commitTransaction(CommitTransactionRequest.newBuilder()
                            .setConnectionId(connectionId)
                            .build()));
            assertEquals(Status.Code.FAILED_PRECONDITION, noTransaction.getStatus().getCode());

            stub.beginTransaction(BeginTransactionRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .build());
            StatusRuntimeException nested = assertThrows(
                    StatusRuntimeException.class,
                    () -> stub.beginTransaction(BeginTransactionRequest.newBuilder()
                            .setConnectionId(connectionId)
                            .build()));
            assertEquals(Status.Code.FAILED_PRECONDITION, nested.getStatus().getCode());

            StatusRuntimeException returning = assertThrows(
                    StatusRuntimeException.class,
                    () -> stub.insert(insert(connectionId).toBuilder()
                            .addReturningFields("id")
                            .build()));
            assertEquals(Status.Code.INVALID_ARGUMENT, returning.getStatus().getCode());
            assertTrue(fixture.executions.isEmpty());

            stub.closeConnection(CloseConnectionRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .build());
        } finally {
            close(channel);
        }

        assertEquals(0, fixture.commits.get());
        assertEquals(1, fixture.rollbacks.get());
        assertTrue(fixture.closed.get());
    }

    private static ProviderServer server(JdbcFixture fixture) {
        ProviderService service = new ProviderService(
                () -> true,
                SchemaMetadata::getDefaultInstance,
                fixture::connection,
                new SchemaPolicy(Set.of("KUBLING"), Set.of()));
        return new ProviderServer(
                new InetSocketAddress("127.0.0.1", 0), Duration.ofSeconds(2), service);
    }

    private static ManagedChannel channel(ProviderServer server) {
        return NettyChannelBuilder.forAddress("127.0.0.1", server.port())
                .usePlaintext()
                .build();
    }

    private static ProviderServiceGrpc.ProviderServiceBlockingStub stub(ManagedChannel channel) {
        return ProviderServiceGrpc.newBlockingStub(channel)
                .withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    private static String open(ProviderServiceGrpc.ProviderServiceBlockingStub stub) {
        OpenConnectionResponse response = stub.openConnection(
                OpenConnectionRequest.getDefaultInstance());
        assertFalse(response.getConnectionId().isBlank());
        return response.getConnectionId();
    }

    private static void close(ManagedChannel channel) throws InterruptedException {
        if (channel != null) {
            channel.shutdownNow();
            assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static InsertRequest insert(String connectionId) {
        return InsertRequest.newBuilder()
                .setConnectionId(connectionId)
                .setEntity(entity())
                .setRows(TupleBatch.newBuilder()
                        .addFields(field("id", ValueType.VALUE_TYPE_LONG))
                        .addFields(field("select", ValueType.VALUE_TYPE_STRING))
                        .addTuples(Tuple.newBuilder()
                                .addValues(Value.newBuilder().setLongValue(7))
                                .addValues(Value.newBuilder().setStringValue("first")))
                        .addTuples(Tuple.newBuilder()
                                .addValues(Value.newBuilder().setLongValue(11))
                                .addValues(Value.newBuilder().setStringValue("second"))))
                .build();
    }

    private static UpdateRequest update(String connectionId) {
        return UpdateRequest.newBuilder()
                .setConnectionId(connectionId)
                .setEntity(entity())
                .addAssignments(Assignment.newBuilder()
                        .setField("Amount")
                        .setValue(decimalLiteral("19.95")))
                .setFilter(comparison(fieldReference("id"), longLiteral(7)))
                .build();
    }

    private static DeleteRequest delete(String connectionId) {
        return DeleteRequest.newBuilder()
                .setConnectionId(connectionId)
                .setEntity(entity())
                .setFilter(comparison(fieldReference("select"), stringLiteral("second")))
                .build();
    }

    private static EntityReference entity() {
        return EntityReference.newBuilder()
                .setNamespace("KUBLING")
                .setName("Order")
                .build();
    }

    private static Field field(String name, ValueType type) {
        return Field.newBuilder()
                .setName(name)
                .setType(type)
                .setTypeDescriptor(TypeDescriptor.newBuilder().setType(type))
                .build();
    }

    private static Expression fieldReference(String name) {
        return Expression.newBuilder()
                .setField(FieldReference.newBuilder().setName(name))
                .build();
    }

    private static Expression comparison(Expression left, Expression right) {
        return Expression.newBuilder()
                .setComparison(ComparisonExpression.newBuilder()
                        .setOperator(ComparisonOperator.COMPARISON_OPERATOR_EQUAL)
                        .setLeft(left)
                        .setRight(right))
                .build();
    }

    private static Expression stringLiteral(String value) {
        return literal(Value.newBuilder().setStringValue(value), ValueType.VALUE_TYPE_STRING);
    }

    private static Expression decimalLiteral(String value) {
        return literal(
                Value.newBuilder().setBigdecimalValue(value),
                ValueType.VALUE_TYPE_BIGDECIMAL);
    }

    private static Expression longLiteral(long value) {
        return literal(Value.newBuilder().setLongValue(value), ValueType.VALUE_TYPE_LONG);
    }

    private static Expression literal(Value.Builder value, ValueType type) {
        return Expression.newBuilder()
                .setLiteral(Literal.newBuilder()
                        .setValue(value)
                        .setDeclaredType(TypeDescriptor.newBuilder().setType(type)))
                .build();
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0.0F;
        }
        if (type == double.class) {
            return 0.0D;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }

    private record Execution(String sql, List<Map<Integer, Object>> parameters) {}

    private static final class JdbcFixture {
        private final AtomicBoolean autoCommit = new AtomicBoolean(true);
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicInteger commits = new AtomicInteger();
        private final AtomicInteger rollbacks = new AtomicInteger();
        private final List<Execution> executions = new ArrayList<>();

        private Connection connection() {
            InvocationHandler handler = (proxy, method, arguments) -> switch (method.getName()) {
                case "getAutoCommit" -> autoCommit.get();
                case "setAutoCommit" -> {
                    autoCommit.set((Boolean) arguments[0]);
                    yield null;
                }
                case "commit" -> {
                    commits.incrementAndGet();
                    yield null;
                }
                case "rollback" -> {
                    rollbacks.incrementAndGet();
                    yield null;
                }
                case "prepareStatement" -> statement((String) arguments[0]);
                case "close" -> {
                    closed.set(true);
                    yield null;
                }
                case "isClosed" -> closed.get();
                default -> defaultValue(method.getReturnType());
            };
            return (Connection) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {Connection.class},
                    handler);
        }

        private PreparedStatement statement(String sql) {
            Map<Integer, Object> parameters = new LinkedHashMap<>();
            List<Map<Integer, Object>> batches = new ArrayList<>();
            InvocationHandler handler = (proxy, method, arguments) -> {
                String name = method.getName();
                if (name.startsWith("set")
                        && arguments != null
                        && arguments.length >= 2
                        && arguments[0] instanceof Integer index) {
                    parameters.put(index, arguments[1]);
                    return null;
                }
                return switch (name) {
                    case "clearParameters" -> {
                        parameters.clear();
                        yield null;
                    }
                    case "addBatch" -> {
                        batches.add(Map.copyOf(parameters));
                        yield null;
                    }
                    case "executeLargeBatch" -> {
                        executions.add(new Execution(sql, List.copyOf(batches)));
                        long[] counts = new long[batches.size()];
                        java.util.Arrays.fill(counts, 1L);
                        yield counts;
                    }
                    case "executeLargeUpdate" -> {
                        executions.add(new Execution(sql, List.of(Map.copyOf(parameters))));
                        yield sql.startsWith("UPDATE") ? 3L : 2L;
                    }
                    case "close", "cancel" -> null;
                    case "isClosed" -> false;
                    default -> defaultValue(method.getReturnType());
                };
            };
            return (PreparedStatement) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {PreparedStatement.class},
                    handler);
        }
    }
}
