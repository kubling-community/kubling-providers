package com.kubling.provider.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kubling.provider.grpc.GetCapabilitiesRequest;
import com.kubling.provider.grpc.GetCapabilitiesResponse;
import com.kubling.provider.grpc.BeginTransactionBranchRequest;
import com.kubling.provider.grpc.BeginTransactionBranchResponse;
import com.kubling.provider.grpc.GetSchemaRequest;
import com.kubling.provider.grpc.GetSchemaResponse;
import com.kubling.provider.grpc.HealthRequest;
import com.kubling.provider.grpc.HealthResponse;
import com.kubling.provider.grpc.NamespaceMetadata;
import com.kubling.provider.grpc.SchemaMetadata;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.sql.SQLException;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class ProviderServiceTest {
    @Test
    void reportsAReachableDatabase() {
        ProviderService service = service(() -> true);
        UnaryObserver<HealthResponse> observer = new UnaryObserver<>();

        service.health(HealthRequest.getDefaultInstance(), observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertTrue(observer.value.getHealthy());
        assertEquals("Oracle database is reachable", observer.value.getMessage());
    }

    @Test
    void sanitizesConnectivityFailures() {
        ProviderService service = service(() -> {
            throw new SQLException(
                    "connection rejected for jdbc:oracle:thin:@private-host using do-not-echo");
        });
        UnaryObserver<HealthResponse> observer = new UnaryObserver<>();

        service.health(HealthRequest.getDefaultInstance(), observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertFalse(observer.value.getHealthy());
        assertEquals("Oracle database is unavailable", observer.value.getMessage());
        assertFalse(observer.value.getMessage().contains("private-host"));
        assertFalse(observer.value.getMessage().contains("do-not-echo"));
    }

    @Test
    void advertisesOnlyImplementedCapabilities() {
        ProviderService service = service(() -> true);
        UnaryObserver<GetCapabilitiesResponse> observer = new UnaryObserver<>();

        service.getCapabilities(GetCapabilitiesRequest.getDefaultInstance(), observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertTrue(observer.value.hasTransactions());
        assertTrue(observer.value.getTransactions().getSupported());
        assertTrue(observer.value.getTransactions().hasRecovery());
        assertFalse(observer.value.getTransactions().getRecovery().getSupported());
        assertTrue(observer.value.hasQuery());
        assertTrue(observer.value.getQuery().getOrdering().getSupported());
        assertTrue(observer.value.getQuery().getOrdering().getExplicitNullOrdering());
        assertTrue(observer.value.getQuery().getPagination().getLimit());
        assertTrue(observer.value.getQuery().getPagination().getOffset());
        assertTrue(observer.value.getQuery().getAggregates().getGroupBy());
        assertTrue(observer.value.getQuery().getAggregates().getHaving());
        assertFalse(observer.value.getQuery().getPartialResults());
        assertTrue(observer.value.hasMutations());
        assertTrue(observer.value.getMutations().getInsert());
        assertTrue(observer.value.getMutations().getUpdate());
        assertTrue(observer.value.getMutations().getDelete());
        assertFalse(observer.value.getMutations().getGeneratedValues());
        assertTrue(observer.value.hasValues());
        assertFalse(observer.value.getValues().getSupportedTypesList().isEmpty());
    }

    @Test
    void advertisesConfiguredDurableRecoveryIdentity() {
        GetCapabilitiesResponse response = Capabilities.response("oracle-rm");

        assertTrue(response.getTransactions().getRecovery().getSupported());
        assertEquals(
                "oracle-rm",
                response.getTransactions().getRecovery().getResourceManagerId());
    }

    @Test
    void rejectsDurableBranchOperationsWhenRecoveryIsNotConfigured() {
        ProviderService service = service(() -> true);
        UnaryObserver<BeginTransactionBranchResponse> observer = new UnaryObserver<>();

        service.beginTransactionBranch(
                BeginTransactionBranchRequest.getDefaultInstance(), observer);

        assertFalse(observer.completed);
        assertEquals(Status.Code.UNIMPLEMENTED, Status.fromThrowable(observer.error).getCode());
    }

    @Test
    void returnsDiscoveredStructuredMetadata() {
        SchemaMetadata metadata = SchemaMetadata.newBuilder()
                .addNamespaces(NamespaceMetadata.newBuilder().setName("APP"))
                .build();
        ProviderService service = new ProviderService(
                () -> true,
                () -> metadata,
                unusedConnectionFactory(),
                schemaPolicy());
        UnaryObserver<GetSchemaResponse> observer = new UnaryObserver<>();

        service.getSchema(GetSchemaRequest.getDefaultInstance(), observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertTrue(observer.value.hasMetadata());
        assertEquals("APP", observer.value.getMetadata().getNamespaces(0).getName());
        assertTrue(observer.value.getSchemaDdl().isEmpty());
    }

    @Test
    void sanitizesSchemaDiscoveryFailures() {
        ProviderService service = new ProviderService(
                () -> true,
                () -> {
                    throw new SQLException("metadata failed for private-host with do-not-echo");
                },
                unusedConnectionFactory(),
                schemaPolicy());
        UnaryObserver<GetSchemaResponse> observer = new UnaryObserver<>();

        service.getSchema(GetSchemaRequest.getDefaultInstance(), observer);

        assertFalse(observer.completed);
        assertNull(observer.value);
        assertEquals(Status.Code.UNAVAILABLE, Status.fromThrowable(observer.error).getCode());
        assertEquals(
                "Oracle schema discovery failed",
                Status.fromThrowable(observer.error).getDescription());
        assertFalse(observer.error.getMessage().contains("private-host"));
        assertFalse(observer.error.getMessage().contains("do-not-echo"));
    }

    private static ProviderService service(ConnectionProbe connectionProbe) {
        return new ProviderService(
                connectionProbe,
                SchemaMetadata::getDefaultInstance,
                unusedConnectionFactory(),
                schemaPolicy());
    }

    private static ConnectionFactory unusedConnectionFactory() {
        return () -> {
            throw new SQLException("unexpected connection open");
        };
    }

    private static SchemaPolicy schemaPolicy() {
        return new SchemaPolicy(Set.of("KUBLING"), Set.of());
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
}
