package com.kubling.provider.oracle;

import com.kubling.provider.grpc.QueryRequest;
import com.kubling.provider.grpc.QueryResponse;
import com.kubling.provider.grpc.TransactionBranchId;
import com.kubling.provider.grpc.TransactionBranchState;
import com.kubling.provider.grpc.TransactionPrepareOutcome;
import io.grpc.stub.StreamObserver;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

final class LogicalConnection implements AutoCloseable {
    private final Connection connection;
    private final AutoCloseable resource;
    private final String connectionId;
    private final String resourceManagerId;
    private final TransactionJournal journal;
    private final XAResource xaResource;
    private final ReentrantLock lock = new ReentrantLock();
    private boolean closed;
    private boolean inTransaction;
    private BranchIdentity activeBranch;

    LogicalConnection(Connection connection) throws SQLException {
        this(connection, connection, null, null, null, null);
    }

    private LogicalConnection(
            Connection connection,
            AutoCloseable resource,
            String connectionId,
            String resourceManagerId,
            TransactionJournal journal,
            XAResource xaResource)
            throws SQLException {
        this.connection = connection;
        this.resource = resource;
        this.connectionId = connectionId;
        this.resourceManagerId = resourceManagerId;
        this.journal = journal;
        this.xaResource = xaResource;
        if (!connection.getAutoCommit()) {
            connection.setAutoCommit(true);
        }
    }

    static LogicalConnection xa(
            String connectionId,
            XaSession session,
            String resourceManagerId,
            TransactionJournal journal)
            throws SQLException {
        return new LogicalConnection(
                session.connection(),
                session,
                connectionId,
                resourceManagerId,
                journal,
                session.xaResource());
    }

    void query(
            JdbcQueryExecutor executor,
            QueryRequest request,
            QueryPlan plan,
            StreamObserver<QueryResponse> observer)
            throws SQLException {
        lock.lock();
        try {
            ensureOpen();
            executor.execute(connection, request, plan, observer);
        } finally {
            lock.unlock();
        }
    }

    OptionalLong insert(JdbcMutationExecutor executor, InsertPlan plan)
            throws SQLException {
        lock.lock();
        try {
            ensureOpen();
            return executor.insert(connection, plan);
        } finally {
            lock.unlock();
        }
    }

    long mutate(JdbcMutationExecutor executor, MutationPlan plan) throws SQLException {
        lock.lock();
        try {
            ensureOpen();
            return executor.execute(connection, plan);
        } finally {
            lock.unlock();
        }
    }

    void beginTransaction() throws SQLException {
        lock.lock();
        try {
            ensureOpen();
            if (inTransaction || activeBranch != null) {
                throw new TransactionStateException(
                        "Oracle logical connection already has an active transaction");
            }
            connection.setAutoCommit(false);
            inTransaction = true;
        } finally {
            lock.unlock();
        }
    }

    void commitTransaction() throws SQLException {
        finishTransaction(true);
    }

    void rollbackTransaction() throws SQLException {
        finishTransaction(false);
    }

    boolean isInTransaction() throws SQLException {
        lock.lock();
        try {
            ensureOpen();
            return inTransaction || activeBranch != null;
        } finally {
            lock.unlock();
        }
    }

    void beginBranch(TransactionBranchId requested) throws SQLException, XAException {
        lock.lock();
        try {
            ensureXaAvailable();
            BranchIdentity identity = BranchIdentity.from(requested, resourceManagerId);
            if (activeBranch != null) {
                if (activeBranch.equals(identity)) {
                    return;
                }
                throw new TransactionStateException(
                        "Oracle logical connection already has an active transaction branch");
            }
            if (inTransaction) {
                throw new TransactionStateException(
                        "Oracle logical connection already has an active local transaction");
            }

            BranchRecord record = new BranchRecord(
                    identity,
                    BranchXid.derive(identity),
                    BranchState.ACTIVE,
                    connectionId);
            if (!journal.insert(record)) {
                BranchRecord existing = journal.find(identity).orElseThrow(() ->
                        new TransactionStateException(
                                "Oracle transaction branch journal entry disappeared"));
                throw new TransactionStateException(
                        "Oracle transaction branch already exists in state "
                                + existing.state().publicState().name());
            }

            try {
                xaResource.start(record.xid(), XAResource.TMNOFLAGS);
                activeBranch = identity;
            } catch (XAException exception) {
                journal.delete(identity);
                throw exception;
            }
        } finally {
            lock.unlock();
        }
    }

    TransactionPrepareOutcome prepareBranch(TransactionBranchId requested)
            throws SQLException, XAException {
        lock.lock();
        try {
            ensureXaAvailable();
            BranchIdentity identity = BranchIdentity.from(requested, resourceManagerId);
            BranchRecord record = journal.find(identity).orElseThrow(() ->
                    new TransactionStateException("Oracle transaction branch was not found"));
            if (!connectionId.equals(record.connectionId())) {
                throw new TransactionStateException(
                        "Oracle transaction branch belongs to another logical connection");
            }
            if (record.state() == BranchState.PREPARED) {
                return TransactionPrepareOutcome.TRANSACTION_PREPARE_OUTCOME_PREPARED;
            }
            if (record.state() == BranchState.READ_ONLY) {
                return TransactionPrepareOutcome.TRANSACTION_PREPARE_OUTCOME_READ_ONLY;
            }
            if (!identity.equals(activeBranch) || record.state() != BranchState.ACTIVE) {
                throw new TransactionStateException(
                        "Oracle transaction branch is not active on this logical connection");
            }
            transition(record, BranchState.PREPARING);
            try {
                xaResource.end(record.xid(), XAResource.TMSUCCESS);
                int outcome = xaResource.prepare(record.xid());
                activeBranch = null;
                if (outcome == XAResource.XA_RDONLY) {
                    transition(
                            record.withState(BranchState.PREPARING),
                            BranchState.READ_ONLY);
                    return TransactionPrepareOutcome.TRANSACTION_PREPARE_OUTCOME_READ_ONLY;
                }
                if (outcome != XAResource.XA_OK) {
                    transition(
                            record.withState(BranchState.PREPARING),
                            BranchState.HEURISTIC_HAZARD);
                    throw new TransactionStateException(
                            "Oracle returned an invalid XA prepare outcome");
                }
                transition(
                        record.withState(BranchState.PREPARING),
                        BranchState.PREPARED);
                return TransactionPrepareOutcome.TRANSACTION_PREPARE_OUTCOME_PREPARED;
            } catch (XAException exception) {
                activeBranch = null;
                poison(exception);
                throw exception;
            }
        } finally {
            lock.unlock();
        }
    }

    TransactionBranchState commitBranch(TransactionBranchId requested)
            throws SQLException, XAException {
        return resolveActiveBranch(requested, true);
    }

    TransactionBranchState rollbackBranch(TransactionBranchId requested)
            throws SQLException, XAException {
        return resolveActiveBranch(requested, false);
    }

    private TransactionBranchState resolveActiveBranch(
            TransactionBranchId requested, boolean commit) throws SQLException, XAException {
        lock.lock();
        try {
            ensureXaAvailable();
            BranchIdentity identity = BranchIdentity.from(requested, resourceManagerId);
            Optional<BranchRecord> existing = journal.find(identity);
            if (existing.isEmpty()) {
                return TransactionBranchState.TRANSACTION_BRANCH_STATE_NOT_FOUND;
            }
            BranchRecord record = existing.orElseThrow();
            if (record.state().isTerminal()) {
                return record.state().publicState();
            }
            if (!connectionId.equals(record.connectionId()) || !identity.equals(activeBranch)) {
                throw new TransactionStateException(
                        "Oracle transaction branch is not active on this logical connection");
            }
            if (record.state() != BranchState.ACTIVE) {
                throw new TransactionStateException(
                        "Oracle transaction branch cannot be resolved from its current state");
            }

            BranchState intent = commit
                    ? BranchState.COMMITTING_ONE_PHASE
                    : BranchState.ROLLING_BACK_ACTIVE;
            transition(record, intent);
            BranchRecord intended = record.withState(intent);
            try {
                xaResource.end(record.xid(), commit ? XAResource.TMSUCCESS : XAResource.TMFAIL);
                if (commit) {
                    xaResource.commit(record.xid(), true);
                } else {
                    xaResource.rollback(record.xid());
                }
                BranchState terminal = commit
                        ? BranchState.COMMITTED
                        : BranchState.ROLLED_BACK;
                transition(intended, terminal);
                activeBranch = null;
                return terminal.publicState();
            } catch (XAException exception) {
                activeBranch = null;
                BranchState heuristic = XaSupport.heuristicState(exception);
                transition(
                        intended,
                        heuristic == null ? BranchState.HEURISTIC_HAZARD : heuristic);
                poison(exception);
                if (heuristic != null) {
                    return heuristic.publicState();
                }
                throw exception;
            }
        } finally {
            lock.unlock();
        }
    }

    private void finishTransaction(boolean commit) throws SQLException {
        lock.lock();
        try {
            ensureOpen();
            if (!inTransaction) {
                throw new TransactionStateException(
                        "Oracle logical connection has no active local transaction");
            }
            if (commit) {
                connection.commit();
            } else {
                connection.rollback();
            }
            inTransaction = false;
            restoreAutoCommit();
        } finally {
            lock.unlock();
        }
    }

    private void transition(BranchRecord record, BranchState nextState)
            throws SQLException {
        if (!journal.transition(record.identity(), Set.of(record.state()), nextState)) {
            Optional<BranchRecord> current = journal.find(record.identity());
            if (current.isEmpty() || current.orElseThrow().state() != nextState) {
                throw new TransactionStateException(
                        "Oracle transaction branch changed concurrently");
            }
        }
    }

    private void restoreAutoCommit() throws SQLException {
        try {
            connection.setAutoCommit(true);
        } catch (SQLException exception) {
            closed = true;
            closeResource(exception);
            throw exception;
        }
    }

    private void ensureOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Oracle logical connection is closed");
        }
    }

    private void ensureXaAvailable() throws SQLException {
        ensureOpen();
        if (xaResource == null) {
            throw new TransactionStateException(
                    "durable Oracle transaction recovery is not configured");
        }
    }

    @Override
    public void close() throws SQLException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            SQLException failure = null;
            if (activeBranch != null) {
                failure = rollbackActiveBranchOnClose();
            } else if (inTransaction) {
                try {
                    connection.rollback();
                } catch (SQLException exception) {
                    failure = exception;
                } finally {
                    inTransaction = false;
                }
            }
            try {
                resource.close();
            } catch (Exception exception) {
                SQLException closeFailure = exception instanceof SQLException sqlException
                        ? sqlException
                        : new SQLException("Unable to close Oracle connection resource", exception);
                if (failure == null) {
                    failure = closeFailure;
                } else {
                    failure.addSuppressed(closeFailure);
                }
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            lock.unlock();
        }
    }

    private SQLException rollbackActiveBranchOnClose() {
        BranchIdentity identity = activeBranch;
        activeBranch = null;
        try {
            BranchRecord record = journal.find(identity).orElseThrow(() ->
                    new TransactionStateException("Oracle transaction branch was not found"));
            transition(record, BranchState.ROLLING_BACK_ACTIVE);
            xaResource.end(record.xid(), XAResource.TMFAIL);
            xaResource.rollback(record.xid());
            transition(
                    record.withState(BranchState.ROLLING_BACK_ACTIVE),
                    BranchState.ROLLED_BACK);
            return null;
        } catch (SQLException exception) {
            return exception;
        } catch (XAException | RuntimeException exception) {
            return new SQLException("Unable to roll back active Oracle XA branch", exception);
        }
    }

    private void closeResource(SQLException failure) {
        try {
            resource.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private void poison(XAException failure) {
        closed = true;
        try {
            resource.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }
}
