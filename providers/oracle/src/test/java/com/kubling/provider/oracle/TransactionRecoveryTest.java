package com.kubling.provider.oracle;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.kubling.provider.grpc.BeginTransactionBranchRequest;
import com.kubling.provider.grpc.CommitTransactionBranchRequest;
import com.kubling.provider.grpc.CloseConnectionRequest;
import com.kubling.provider.grpc.ForgetTransactionBranchRequest;
import com.kubling.provider.grpc.GetCapabilitiesRequest;
import com.kubling.provider.grpc.GetTransactionBranchStatusRequest;
import com.kubling.provider.grpc.OpenConnectionRequest;
import com.kubling.provider.grpc.PrepareTransactionBranchRequest;
import com.kubling.provider.grpc.ProviderServiceGrpc;
import com.kubling.provider.grpc.RecoverTransactionBranchesRequest;
import com.kubling.provider.grpc.RollbackTransactionBranchRequest;
import com.kubling.provider.grpc.SchemaMetadata;
import com.kubling.provider.grpc.TransactionBranchId;
import com.kubling.provider.grpc.TransactionBranchState;
import com.kubling.provider.grpc.TransactionPrepareOutcome;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.junit.jupiter.api.Test;

final class TransactionRecoveryTest {
    private static final String RESOURCE_MANAGER_ID = "oracle-test-rm";

    @Test
    void recoversAndResolvesAPreparedBranchAfterProviderRestart() throws Exception {
        InMemoryJournal journal = new InMemoryJournal();
        FakeResourceManager resourceManager = new FakeResourceManager();
        TransactionBranchId branch = branch(new byte[] {0, 1, 2, (byte) 255});

        try (RunningProvider first = start(journal, resourceManager)) {
            ProviderServiceGrpc.ProviderServiceBlockingStub stub = first.stub();
            String connectionId = stub.openConnection(OpenConnectionRequest.getDefaultInstance())
                    .getConnectionId();
            assertTrue(stub.getCapabilities(GetCapabilitiesRequest.getDefaultInstance())
                    .getTransactions()
                    .getRecovery()
                    .getSupported());

            assertEquals(
                    TransactionBranchState.TRANSACTION_BRANCH_STATE_ACTIVE,
                    stub.beginTransactionBranch(BeginTransactionBranchRequest.newBuilder()
                                    .setConnectionId(connectionId)
                                    .setBranch(branch)
                                    .build())
                            .getState());
            assertEquals(
                    TransactionPrepareOutcome.TRANSACTION_PREPARE_OUTCOME_PREPARED,
                    stub.prepareTransactionBranch(PrepareTransactionBranchRequest.newBuilder()
                                    .setConnectionId(connectionId)
                                    .setBranch(branch)
                                    .build())
                            .getOutcome());
        }

        try (RunningProvider restarted = start(journal, resourceManager)) {
            ProviderServiceGrpc.ProviderServiceBlockingStub stub = restarted.stub();
            Iterator<com.kubling.provider.grpc.RecoverTransactionBranchesResponse> recovery =
                    stub.recoverTransactionBranches(RecoverTransactionBranchesRequest.newBuilder()
                            .setResourceManagerId(RESOURCE_MANAGER_ID)
                            .build());
            assertTrue(recovery.hasNext());
            var recovered = recovery.next().getBranchesList();
            assertEquals(1, recovered.size());
            assertEquals(branch, recovered.getFirst().getBranch());
            assertEquals(
                    TransactionBranchState.TRANSACTION_BRANCH_STATE_PREPARED,
                    recovered.getFirst().getState());
            assertFalse(recovery.hasNext());

            assertEquals(
                    TransactionBranchState.TRANSACTION_BRANCH_STATE_COMMITTED,
                    stub.commitTransactionBranch(CommitTransactionBranchRequest.newBuilder()
                                    .setBranch(branch)
                                    .build())
                            .getState());
            assertEquals(
                    TransactionBranchState.TRANSACTION_BRANCH_STATE_COMMITTED,
                    stub.getTransactionBranchStatus(GetTransactionBranchStatusRequest.newBuilder()
                                    .setBranch(branch)
                                    .build())
                            .getState());

            stub.forgetTransactionBranch(ForgetTransactionBranchRequest.newBuilder()
                    .setBranch(branch)
                    .build());
            assertEquals(
                    TransactionBranchState.TRANSACTION_BRANCH_STATE_NOT_FOUND,
                    stub.getTransactionBranchStatus(GetTransactionBranchStatusRequest.newBuilder()
                                    .setBranch(branch)
                                    .build())
                            .getState());
        }
    }

    @Test
    void reconcilesCrashesAroundPrepareAndResolution() throws Exception {
        InMemoryJournal journal = new InMemoryJournal();
        FakeResourceManager resourceManager = new FakeResourceManager();

        BranchRecord preparing = record(new byte[] {1}, BranchState.PREPARING);
        journal.insert(preparing);
        resourceManager.prepared(preparing.xid());

        BranchRecord committing = record(new byte[] {2}, BranchState.COMMITTING);
        journal.insert(committing);
        resourceManager.prepared(committing.xid());

        BranchRecord missingPrepared = record(new byte[] {3}, BranchState.PREPARED);
        journal.insert(missingPrepared);

        BranchRecord ambiguousOnePhase =
                record(new byte[] {4}, BranchState.COMMITTING_ONE_PHASE);
        journal.insert(ambiguousOnePhase);

        TransactionRecoveryManager manager = manager(journal, resourceManager);
        manager.validate();

        assertEquals(
                TransactionBranchState.TRANSACTION_BRANCH_STATE_PREPARED,
                manager.status(preparing.identity()));
        assertEquals(
                TransactionBranchState.TRANSACTION_BRANCH_STATE_COMMITTED,
                manager.status(committing.identity()));
        assertEquals(
                TransactionBranchState.TRANSACTION_BRANCH_STATE_HEURISTIC_HAZARD,
                manager.status(missingPrepared.identity()));
        assertEquals(
                TransactionBranchState.TRANSACTION_BRANCH_STATE_HEURISTIC_HAZARD,
                manager.status(ambiguousOnePhase.identity()));
    }

    @Test
    void supportsOnePhaseCommitAndRollsBackAnActiveBranchOnConnectionClose()
            throws Exception {
        InMemoryJournal journal = new InMemoryJournal();
        FakeResourceManager resourceManager = new FakeResourceManager();
        TransactionBranchId committed = branch(new byte[] {11});
        TransactionBranchId rolledBack = branch(new byte[] {12});

        try (RunningProvider provider = start(journal, resourceManager)) {
            ProviderServiceGrpc.ProviderServiceBlockingStub stub = provider.stub();
            String connectionId = stub.openConnection(OpenConnectionRequest.getDefaultInstance())
                    .getConnectionId();

            stub.beginTransactionBranch(BeginTransactionBranchRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .setBranch(committed)
                    .build());
            assertEquals(
                    TransactionBranchState.TRANSACTION_BRANCH_STATE_COMMITTED,
                    stub.commitTransactionBranch(CommitTransactionBranchRequest.newBuilder()
                                    .setConnectionId(connectionId)
                                    .setOnePhase(true)
                                    .setBranch(committed)
                                    .build())
                            .getState());
            assertEquals(
                    TransactionBranchState.TRANSACTION_BRANCH_STATE_COMMITTED,
                    stub.rollbackTransactionBranch(RollbackTransactionBranchRequest.newBuilder()
                                    .setConnectionId(connectionId)
                                    .setBranch(committed)
                                    .build())
                            .getState());

            stub.beginTransactionBranch(BeginTransactionBranchRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .setBranch(rolledBack)
                    .build());
            stub.closeConnection(CloseConnectionRequest.newBuilder()
                    .setConnectionId(connectionId)
                    .build());
            assertEquals(
                    TransactionBranchState.TRANSACTION_BRANCH_STATE_ROLLED_BACK,
                    stub.getTransactionBranchStatus(GetTransactionBranchStatusRequest.newBuilder()
                                    .setBranch(rolledBack)
                                    .build())
                            .getState());
        }
    }

    @Test
    void retainsHeuristicOutcomeAndRejectsInvalidBranchIdentity() throws Exception {
        InMemoryJournal journal = new InMemoryJournal();
        FakeResourceManager resourceManager = new FakeResourceManager();
        BranchRecord prepared = record(new byte[] {5}, BranchState.PREPARED);
        journal.insert(prepared);
        resourceManager.prepared(prepared.xid());
        resourceManager.commitFailure = xa(XAException.XA_HEURRB);

        TransactionRecoveryManager manager = manager(journal, resourceManager);
        assertEquals(
                TransactionBranchState.TRANSACTION_BRANCH_STATE_HEURISTIC_ROLLBACK,
                manager.commit(prepared.identity()));
        assertEquals(1, manager.recover().size());

        try (RunningProvider provider = start(journal, resourceManager)) {
            StatusRuntimeException wrongManager = assertThrows(
                    StatusRuntimeException.class,
                    () -> provider.stub().getTransactionBranchStatus(
                            GetTransactionBranchStatusRequest.newBuilder()
                                    .setBranch(TransactionBranchId.newBuilder()
                                            .setResourceManagerId("another-rm")
                                            .setBranchId(ByteString.copyFromUtf8("branch")))
                                    .build()));
            assertEquals(Status.Code.INVALID_ARGUMENT, wrongManager.getStatus().getCode());

            StatusRuntimeException oversized = assertThrows(
                    StatusRuntimeException.class,
                    () -> provider.stub().getTransactionBranchStatus(
                            GetTransactionBranchStatusRequest.newBuilder()
                                    .setBranch(branch(new byte[257]))
                                    .build()));
            assertEquals(Status.Code.INVALID_ARGUMENT, oversized.getStatus().getCode());

            StatusRuntimeException nonCanonicalScan = assertThrows(
                    StatusRuntimeException.class,
                    () -> provider.stub()
                            .recoverTransactionBranches(
                                    RecoverTransactionBranchesRequest.newBuilder()
                                            .setResourceManagerId(" oracle-test-rm")
                                            .build())
                            .hasNext());
            assertEquals(
                    Status.Code.INVALID_ARGUMENT,
                    nonCanonicalScan.getStatus().getCode());
        }
    }

    @Test
    void derivesStableXaIdentityWithoutLosingOpaqueBranchBytes() {
        byte[] opaque = new byte[256];
        for (int index = 0; index < opaque.length; index++) {
            opaque[index] = (byte) index;
        }
        BranchIdentity identity = BranchIdentity.from(branch(opaque), RESOURCE_MANAGER_ID);
        BranchXid first = BranchXid.derive(identity);
        BranchXid second = BranchXid.derive(identity);

        assertEquals(first, second);
        assertEquals(32, first.getGlobalTransactionId().length);
        assertEquals(32, first.getBranchQualifier().length);
        assertArrayEquals(opaque, identity.toProto().getBranchId().toByteArray());
    }

    private static RunningProvider start(
            InMemoryJournal journal, FakeResourceManager resourceManager) throws Exception {
        TransactionRecoveryManager manager = manager(journal, resourceManager);
        manager.validate();
        XaSessionFactory sessions = () -> session(resourceManager);
        ProviderService service = new ProviderService(
                () -> true,
                SchemaMetadata::getDefaultInstance,
                new XaLogicalConnectionFactory(
                        sessions, RESOURCE_MANAGER_ID, journal),
                new SchemaPolicy(Set.of(), Set.of()),
                manager);
        ProviderServer server = new ProviderServer(
                new InetSocketAddress("127.0.0.1", 0), Duration.ofSeconds(2), service);
        server.start();
        ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", server.port())
                .usePlaintext()
                .build();
        return new RunningProvider(server, channel);
    }

    private static TransactionRecoveryManager manager(
            InMemoryJournal journal, FakeResourceManager resourceManager) {
        return new TransactionRecoveryManager(
                RESOURCE_MANAGER_ID, journal, () -> session(resourceManager));
    }

    private static XaSession session(FakeResourceManager resourceManager) throws SQLException {
        return new XaSession(new FakeXaConnection(resourceManager));
    }

    private static TransactionBranchId branch(byte[] id) {
        return TransactionBranchId.newBuilder()
                .setResourceManagerId(RESOURCE_MANAGER_ID)
                .setBranchId(ByteString.copyFrom(id))
                .build();
    }

    private static BranchRecord record(byte[] id, BranchState state) {
        BranchIdentity identity = BranchIdentity.from(branch(id), RESOURCE_MANAGER_ID);
        return new BranchRecord(identity, BranchXid.derive(identity), state, "old-connection");
    }

    private static XAException xa(int code) {
        XAException exception = new XAException(code);
        exception.errorCode = code;
        return exception;
    }

    private record RunningProvider(ProviderServer server, ManagedChannel channel)
            implements AutoCloseable {
        ProviderServiceGrpc.ProviderServiceBlockingStub stub() {
            return ProviderServiceGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            server.close();
        }
    }

    private static final class InMemoryJournal implements TransactionJournal {
        private final Map<BranchIdentity, BranchRecord> records =
                new LinkedHashMap<>();

        @Override
        public void validate() {}

        @Override
        public synchronized boolean insert(BranchRecord record) {
            return records.putIfAbsent(record.identity(), record) == null;
        }

        @Override
        public synchronized Optional<BranchRecord> find(BranchIdentity identity) {
            return Optional.ofNullable(records.get(identity));
        }

        @Override
        public synchronized List<BranchRecord> pending() {
            return records.values().stream()
                    .filter(record -> record.state() != BranchState.READ_ONLY)
                    .filter(record -> record.state() != BranchState.COMMITTED)
                    .filter(record -> record.state() != BranchState.ROLLED_BACK)
                    .toList();
        }

        @Override
        public synchronized boolean transition(
                BranchIdentity identity,
                Set<BranchState> expected,
                BranchState nextState) {
            BranchRecord current = records.get(identity);
            if (current == null || !expected.contains(current.state())) {
                return false;
            }
            records.put(identity, current.withState(nextState));
            return true;
        }

        @Override
        public synchronized boolean delete(BranchIdentity identity) {
            return records.remove(identity) != null;
        }
    }

    private enum NativeState {
        ACTIVE,
        ENDED,
        PREPARED
    }

    private static final class FakeResourceManager {
        private final Map<BranchXid, NativeState> branches = new LinkedHashMap<>();
        private XAException commitFailure;

        synchronized void prepared(BranchXid xid) {
            branches.put(xid, NativeState.PREPARED);
        }
    }

    private static final class FakeXaConnection implements XAConnection {
        private final FakeResourceManager resourceManager;
        private final Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getAutoCommit" -> true;
                    case "isClosed", "isReadOnly", "isWrapperFor" -> false;
                    case "close", "setAutoCommit", "clearWarnings", "abort" -> null;
                    case "unwrap" -> null;
                    default -> defaultValue(method.getReturnType());
                });

        FakeXaConnection(FakeResourceManager resourceManager) {
            this.resourceManager = resourceManager;
        }

        @Override
        public XAResource getXAResource() {
            return new FakeXaResource(resourceManager);
        }

        @Override
        public Connection getConnection() {
            return connection;
        }

        @Override
        public void close() {}

        @Override
        public void addConnectionEventListener(ConnectionEventListener listener) {}

        @Override
        public void removeConnectionEventListener(ConnectionEventListener listener) {}

        @Override
        public void addStatementEventListener(StatementEventListener listener) {}

        @Override
        public void removeStatementEventListener(StatementEventListener listener) {}
    }

    private static final class FakeXaResource implements XAResource {
        private final FakeResourceManager resourceManager;

        FakeXaResource(FakeResourceManager resourceManager) {
            this.resourceManager = resourceManager;
        }

        @Override
        public void start(Xid xid, int flags) {
            synchronized (resourceManager) {
                resourceManager.branches.put(copy(xid), NativeState.ACTIVE);
            }
        }

        @Override
        public void end(Xid xid, int flags) {
            synchronized (resourceManager) {
                resourceManager.branches.put(copy(xid), NativeState.ENDED);
            }
        }

        @Override
        public int prepare(Xid xid) {
            synchronized (resourceManager) {
                resourceManager.branches.put(copy(xid), NativeState.PREPARED);
            }
            return XA_OK;
        }

        @Override
        public void commit(Xid xid, boolean onePhase) throws XAException {
            synchronized (resourceManager) {
                if (resourceManager.commitFailure != null) {
                    XAException failure = resourceManager.commitFailure;
                    resourceManager.commitFailure = null;
                    throw failure;
                }
                resourceManager.branches.remove(copy(xid));
            }
        }

        @Override
        public void rollback(Xid xid) {
            synchronized (resourceManager) {
                resourceManager.branches.remove(copy(xid));
            }
        }

        @Override
        public Xid[] recover(int flag) {
            if (flag != TMSTARTRSCAN) {
                return new Xid[0];
            }
            synchronized (resourceManager) {
                return resourceManager.branches.entrySet().stream()
                        .filter(entry -> entry.getValue() == NativeState.PREPARED)
                        .map(Map.Entry::getKey)
                        .toArray(Xid[]::new);
            }
        }

        @Override
        public void forget(Xid xid) {
            synchronized (resourceManager) {
                resourceManager.branches.remove(copy(xid));
            }
        }

        @Override
        public boolean isSameRM(XAResource other) {
            return other instanceof FakeXaResource fake
                    && fake.resourceManager == resourceManager;
        }

        @Override
        public int getTransactionTimeout() {
            return 0;
        }

        @Override
        public boolean setTransactionTimeout(int seconds) {
            return true;
        }

        private static BranchXid copy(Xid xid) {
            return new BranchXid(
                    xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
        }
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
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        if (type == char.class) {
            return '\0';
        }
        return null;
    }
}
