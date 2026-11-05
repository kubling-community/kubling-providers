package com.kubling.provider.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kubling.provider.grpc.CloseConnectionRequest;
import com.kubling.provider.grpc.CloseConnectionResponse;
import com.kubling.provider.grpc.ComparisonExpression;
import com.kubling.provider.grpc.ComparisonOperator;
import com.kubling.provider.grpc.EntityReference;
import com.kubling.provider.grpc.Expression;
import com.kubling.provider.grpc.FieldReference;
import com.kubling.provider.grpc.Literal;
import com.kubling.provider.grpc.OpenConnectionRequest;
import com.kubling.provider.grpc.OpenConnectionResponse;
import com.kubling.provider.grpc.Projection;
import com.kubling.provider.grpc.ProviderServiceGrpc;
import com.kubling.provider.grpc.QueryCompletion;
import com.kubling.provider.grpc.QueryRequest;
import com.kubling.provider.grpc.QueryResponse;
import com.kubling.provider.grpc.SchemaMetadata;
import com.kubling.transport.grpc.TypeDescriptor;
import com.kubling.transport.grpc.Value;
import com.kubling.transport.grpc.ValueType;
import io.grpc.Status;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import org.junit.jupiter.api.Test;

final class ProviderQueryTest {
    @Test
    void streamsTypedBatchesAndACompleteOutcome() throws Exception {
        JdbcFixture fixture = new JdbcFixture(rows());
        ProviderService service = service(fixture.connection());
        ManagedChannel channel = null;
        List<QueryResponse> responses = new ArrayList<>();
        try (ProviderServer server = new ProviderServer(
                new InetSocketAddress("127.0.0.1", 0), Duration.ofSeconds(2), service)) {
            server.start();
            channel = NettyChannelBuilder.forAddress("127.0.0.1", server.port())
                    .usePlaintext()
                    .build();
            ProviderServiceGrpc.ProviderServiceBlockingStub stub =
                    ProviderServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(5, TimeUnit.SECONDS);
            OpenConnectionResponse opened = stub.openConnection(
                    OpenConnectionRequest.getDefaultInstance());
            assertFalse(opened.getConnectionId().isBlank());

            QueryRequest request = QueryRequest.newBuilder()
                    .setConnectionId(opened.getConnectionId())
                    .setEntity(EntityReference.newBuilder()
                            .setNamespace("KUBLING")
                            .setName("Order"))
                    .addProjections(projection("id"))
                    .addProjections(projection("select"))
                    .setFilter(Expression.newBuilder()
                            .setComparison(ComparisonExpression.newBuilder()
                                    .setOperator(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN)
                                    .setLeft(field("id"))
                                    .setRight(longLiteral(5))))
                    .setLimit(2)
                    .setBatchSize(1)
                    .setAcceptOutcome(true)
                    .build();
            stub.query(request).forEachRemaining(responses::add);
            stub.closeConnection(CloseConnectionRequest.newBuilder()
                    .setConnectionId(opened.getConnectionId())
                    .build());
        } finally {
            if (channel != null) {
                channel.shutdownNow();
                assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            }
        }

        assertEquals(
                "SELECT \"id\" AS \"KBL_C0\", \"select\" AS \"KBL_C1\" "
                        + "FROM \"KUBLING\".\"Order\" "
                        + "WHERE (\"id\" > ?) FETCH FIRST ? ROWS ONLY",
                fixture.sql.get());
        assertEquals(1, fixture.fetchSize.get());
        assertEquals(5L, fixture.parameters.get(1));
        assertEquals(2L, fixture.parameters.get(2));
        assertEquals(3, responses.size());
        assertTrue(responses.get(0).hasBatch());
        assertTrue(responses.get(1).hasBatch());
        assertTrue(responses.get(2).hasOutcome());
        assertEquals(
                QueryCompletion.QUERY_COMPLETION_COMPLETE,
                responses.get(2).getOutcome().getCompletion());

        List<String> ids = new ArrayList<>();
        for (int index = 0; index < 2; index++) {
            QueryResponse response = responses.get(index);
            assertEquals(1, response.getBatch().getTuplesCount());
            assertEquals(ValueType.VALUE_TYPE_BIGINTEGER, response.getBatch().getFields(0).getType());
            assertEquals(ValueType.VALUE_TYPE_STRING, response.getBatch().getFields(1).getType());
            ids.add(response.getBatch().getTuples(0).getValues(0).getBigintegerValue());
        }
        assertEquals(Set.of("7", "11"), Set.copyOf(ids));

        assertTrue(fixture.closed.get());
    }

    @Test
    void rejectsInvalidRoutingBeforePreparingSql() {
        JdbcFixture fixture = new JdbcFixture(null);
        ProviderService service = service(fixture.connection());

        StreamingObserver<QueryResponse> missingConnection = new StreamingObserver<>();
        service.query(QueryRequest.getDefaultInstance(), missingConnection);
        assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(missingConnection.error).getCode());

        StreamingObserver<QueryResponse> unknownConnection = new StreamingObserver<>();
        service.query(
                QueryRequest.newBuilder().setConnectionId("missing").build(),
                unknownConnection);
        assertEquals(Status.Code.NOT_FOUND, Status.fromThrowable(unknownConnection.error).getCode());

        UnaryObserver<OpenConnectionResponse> opened = new UnaryObserver<>();
        service.openConnection(OpenConnectionRequest.getDefaultInstance(), opened);
        StreamingObserver<QueryResponse> deniedNamespace = new StreamingObserver<>();
        service.query(
                QueryRequest.newBuilder()
                        .setConnectionId(opened.value.getConnectionId())
                        .setEntity(EntityReference.newBuilder()
                                .setNamespace("SYS")
                                .setName("USER$"))
                        .addProjections(projection("NAME"))
                        .build(),
                deniedNamespace);
        assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(deniedNamespace.error).getCode());
        assertNull(fixture.sql.get());
        service.close();
    }

    private static ProviderService service(Connection connection) {
        return new ProviderService(
                () -> true,
                SchemaMetadata::getDefaultInstance,
                () -> connection,
                new SchemaPolicy(Set.of("KUBLING"), Set.of()));
    }

    private static Projection projection(String name) {
        return Projection.newBuilder()
                .setExpression(field(name))
                .setOutputName(name)
                .build();
    }

    private static Expression field(String name) {
        return Expression.newBuilder()
                .setField(FieldReference.newBuilder().setName(name))
                .build();
    }

    private static Expression longLiteral(long value) {
        return Expression.newBuilder()
                .setLiteral(Literal.newBuilder()
                        .setValue(Value.newBuilder().setLongValue(value))
                        .setDeclaredType(TypeDescriptor.newBuilder()
                                .setType(ValueType.VALUE_TYPE_LONG)))
                .build();
    }

    private static CachedRowSet rows() throws SQLException {
        RowSetMetaDataImpl metadata = new RowSetMetaDataImpl();
        metadata.setColumnCount(2);
        metadata.setColumnName(1, "id");
        metadata.setColumnLabel(1, "id");
        metadata.setColumnType(1, Types.NUMERIC);
        metadata.setColumnTypeName(1, "NUMBER");
        metadata.setPrecision(1, 19);
        metadata.setScale(1, 0);
        metadata.setColumnName(2, "select");
        metadata.setColumnLabel(2, "select");
        metadata.setColumnType(2, Types.VARCHAR);
        metadata.setColumnTypeName(2, "VARCHAR2");
        metadata.setPrecision(2, 120);
        metadata.setScale(2, 0);

        CachedRowSet rows = RowSetProvider.newFactory().createCachedRowSet();
        rows.setMetaData(metadata);
        insert(rows, 7, "first");
        insert(rows, 11, "second");
        rows.beforeFirst();
        return rows;
    }

    private static void insert(CachedRowSet rows, long id, String name) throws SQLException {
        rows.moveToInsertRow();
        rows.updateLong(1, id);
        rows.updateString(2, name);
        rows.insertRow();
        rows.moveToCurrentRow();
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

    private static final class JdbcFixture {
        private final ResultSet rows;
        private final AtomicReference<String> sql = new AtomicReference<>();
        private final AtomicInteger fetchSize = new AtomicInteger();
        private final Map<Integer, Object> parameters = new LinkedHashMap<>();
        private final AtomicBoolean closed = new AtomicBoolean();

        private JdbcFixture(ResultSet rows) {
            this.rows = rows;
        }

        private Connection connection() {
            PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {PreparedStatement.class},
                    statementHandler());
            InvocationHandler handler = (proxy, method, arguments) -> switch (method.getName()) {
                case "prepareStatement" -> {
                    sql.set((String) arguments[0]);
                    yield statement;
                }
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

        private InvocationHandler statementHandler() {
            return (proxy, method, arguments) -> {
                String name = method.getName();
                if (name.equals("setFetchSize")) {
                    fetchSize.set((Integer) arguments[0]);
                    return null;
                }
                if (name.startsWith("set")
                        && arguments != null
                        && arguments.length >= 2
                        && arguments[0] instanceof Integer index) {
                    parameters.put(index, arguments[1]);
                    return null;
                }
                return switch (name) {
                    case "executeQuery" -> rows;
                    case "close", "cancel" -> null;
                    case "isClosed" -> false;
                    default -> defaultValue(method.getReturnType());
                };
            };
        }
    }

    private static final class UnaryObserver<T> implements StreamObserver<T> {
        private T value;
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T nextValue) {
            value = nextValue;
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }

    private static final class StreamingObserver<T> implements StreamObserver<T> {
        private final List<T> values = new ArrayList<>();
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable throwable) {
            error = throwable;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }
}
