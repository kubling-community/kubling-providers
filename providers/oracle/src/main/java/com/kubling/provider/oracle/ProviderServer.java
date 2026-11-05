package com.kubling.provider.oracle;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProviderServer implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProviderServer.class);
    private static final Duration FORCED_SHUTDOWN_WAIT = Duration.ofSeconds(5);

    private final InetSocketAddress listenAddress;
    private final Duration shutdownGracePeriod;
    private final BindableService service;
    private Server server;
    private boolean serviceClosed;

    ProviderServer(
            InetSocketAddress listenAddress,
            Duration shutdownGracePeriod,
            BindableService service) {
        this.listenAddress = listenAddress;
        this.shutdownGracePeriod = shutdownGracePeriod;
        this.service = service;
    }

    synchronized void start() throws IOException {
        if (server != null) {
            throw new IllegalStateException("Oracle provider server is already started");
        }
        server = NettyServerBuilder.forAddress(listenAddress)
                .addService(service)
                .build()
                .start();
    }

    void awaitTermination() throws InterruptedException {
        currentServer().awaitTermination();
    }

    int port() {
        return currentServer().getPort();
    }

    @Override
    public void close() {
        Server runningServer;
        synchronized (this) {
            runningServer = server;
        }
        if (runningServer == null) {
            closeService();
            return;
        }
        try {
            if (!runningServer.isTerminated()) {
                runningServer.shutdown();
                if (!runningServer.awaitTermination(
                        shutdownGracePeriod.toMillis(), TimeUnit.MILLISECONDS)) {
                    LOGGER.warn("Oracle provider did not stop within its shutdown grace period");
                    runningServer.shutdownNow();
                    runningServer.awaitTermination(
                            FORCED_SHUTDOWN_WAIT.toMillis(), TimeUnit.MILLISECONDS);
                }
            }
        } catch (InterruptedException exception) {
            runningServer.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            closeService();
        }
    }

    private synchronized void closeService() {
        if (serviceClosed || !(service instanceof AutoCloseable closeable)) {
            return;
        }
        serviceClosed = true;
        try {
            closeable.close();
        } catch (Exception exception) {
            LOGGER.warn(
                    "Oracle provider resources could not be closed cleanly ({})",
                    exception.getClass().getSimpleName());
        }
    }

    private synchronized Server currentServer() {
        if (server == null) {
            throw new IllegalStateException("Oracle provider server is not started");
        }
        return server;
    }
}
