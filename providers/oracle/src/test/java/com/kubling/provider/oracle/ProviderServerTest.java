package com.kubling.provider.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kubling.provider.grpc.GetSchemaRequest;
import com.kubling.provider.grpc.GetSchemaResponse;
import com.kubling.provider.grpc.HealthRequest;
import com.kubling.provider.grpc.HealthResponse;
import com.kubling.provider.grpc.NamespaceMetadata;
import com.kubling.provider.grpc.ProviderServiceGrpc;
import com.kubling.provider.grpc.SchemaMetadata;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

final class ProviderServerTest {
    @Test
    void servesTheProviderContractAndShutsDown() throws Exception {
        SchemaMetadata schema = SchemaMetadata.newBuilder()
                .addNamespaces(NamespaceMetadata.newBuilder().setName("APP"))
                .build();
        ProviderService service = new ProviderService(
                () -> true,
                () -> schema,
                () -> {
                    throw new SQLException("unexpected connection open");
                },
                new SchemaPolicy(Set.of("APP"), Set.of()));
        ManagedChannel channel = null;

        try (ProviderServer server = new ProviderServer(
                new InetSocketAddress("127.0.0.1", 0), Duration.ofSeconds(2), service)) {
            server.start();
            channel = NettyChannelBuilder.forAddress("127.0.0.1", server.port())
                    .usePlaintext()
                    .build();

            HealthResponse response = ProviderServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .health(HealthRequest.getDefaultInstance());

            assertTrue(response.getHealthy());
            assertEquals("Oracle database is reachable", response.getMessage());

            GetSchemaResponse schemaResponse = ProviderServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getSchema(GetSchemaRequest.getDefaultInstance());
            assertTrue(schemaResponse.hasMetadata());
            assertEquals("APP", schemaResponse.getMetadata().getNamespaces(0).getName());
        } finally {
            if (channel != null) {
                channel.shutdownNow();
                assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }
}
