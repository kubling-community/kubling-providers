package com.kubling.provider.oracle;

import com.kubling.provider.grpc.TransactionBranch;
import com.kubling.provider.grpc.TransactionBranchState;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

final class TransactionRecoveryManager implements AutoCloseable {
    private final String resourceManagerId;
    private final TransactionJournal journal;
    private final XaSessionFactory sessionFactory;
    private final AutoCloseable lease;

    TransactionRecoveryManager(
            String resourceManagerId,
            TransactionJournal journal,
            XaSessionFactory sessionFactory) {
        this(resourceManagerId, journal, sessionFactory, () -> {});
    }

    TransactionRecoveryManager(
            String resourceManagerId,
            TransactionJournal journal,
            XaSessionFactory sessionFactory,
            AutoCloseable lease) {
        this.resourceManagerId = resourceManagerId;
        this.journal = journal;
        this.sessionFactory = sessionFactory;
        this.lease = lease;
    }

    String resourceManagerId() {
        return resourceManagerId;
    }

    void validate() throws SQLException, XAException {
        try {
            journal.validate();
            reconcileAfterRestart();
        } catch (SQLException | XAException | RuntimeException exception) {
            try {
                lease.close();
            } catch (Exception closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }

    void reconcileAfterRestart() throws SQLException, XAException {
        Set<BranchXid> recovered = recoveredXids();
        for (BranchRecord record : journal.pending()) {
            boolean present = recovered.contains(record.xid());
            switch (record.state()) {
                case ACTIVE -> transition(record, BranchState.ROLLED_BACK);
                case PREPARING -> transition(
                        record,
                        present ? BranchState.PREPARED : BranchState.HEURISTIC_HAZARD);
                case PREPARED -> {
                    if (!present) {
                        transition(record, BranchState.HEURISTIC_HAZARD);
                    }
                }
                case COMMITTING -> {
                    if (present) {
                        resolveRecovered(record, true);
                    } else {
                        transition(record, BranchState.COMMITTED);
                    }
                }
                case COMMITTING_ONE_PHASE ->
                    transition(record, BranchState.HEURISTIC_HAZARD);
                case ROLLING_BACK -> {
                    if (present) {
                        resolveRecovered(record, false);
                    } else {
                        transition(record, BranchState.ROLLED_BACK);
                    }
                }
                case ROLLING_BACK_ACTIVE -> {
                    if (present) {
                        resolveRecovered(record, false);
                    } else {
                        transition(record, BranchState.ROLLED_BACK);
                    }
                }
                default -> {
                    // Stable states need no startup repair.
                }
            }
        }
    }

    TransactionBranchState commit(BranchIdentity identity) throws SQLException, XAException {
        return resolve(identity, true);
    }

    TransactionBranchState rollback(BranchIdentity identity)
            throws SQLException, XAException {
        return resolve(identity, false);
    }

    List<TransactionBranch> recover() throws SQLException {
        List<TransactionBranch> branches = new ArrayList<>();
        for (BranchRecord record : journal.pending()) {
            if (record.state().isRecoverable()) {
                branches.add(TransactionBranch.newBuilder()
                        .setBranch(record.identity().toProto())
                        .setState(record.state().publicState())
                        .build());
            }
        }
        return List.copyOf(branches);
    }

    TransactionBranchState status(BranchIdentity identity) throws SQLException {
        return journal.find(identity)
                .map(record -> record.state().publicState())
                .orElse(TransactionBranchState.TRANSACTION_BRANCH_STATE_NOT_FOUND);
    }

    void forget(BranchIdentity identity) throws SQLException, XAException {
        Optional<BranchRecord> existing = journal.find(identity);
        if (existing.isEmpty()) {
            return;
        }
        BranchRecord record = existing.orElseThrow();
        if (!record.state().isTerminal()) {
            throw new TransactionStateException(
                    "active or prepared Oracle transaction branches cannot be forgotten");
        }
        if (record.state().name().startsWith("HEURISTIC_")) {
            try (XaSession session = sessionFactory.openSession()) {
                try {
                    session.xaResource().forget(record.xid());
                } catch (XAException exception) {
                    if (exception.errorCode != XAException.XAER_NOTA) {
                        throw exception;
                    }
                }
            }
        }
        journal.delete(identity);
    }

    private TransactionBranchState resolve(BranchIdentity identity, boolean commit)
            throws SQLException, XAException {
        Optional<BranchRecord> existing = journal.find(identity);
        if (existing.isEmpty()) {
            return TransactionBranchState.TRANSACTION_BRANCH_STATE_NOT_FOUND;
        }
        BranchRecord record = existing.orElseThrow();
        if (record.state().isTerminal()) {
            return record.state().publicState();
        }
        if (record.state() == BranchState.COMMITTING) {
            return finishInterruptedResolution(record, true);
        }
        if (record.state() == BranchState.ROLLING_BACK) {
            return finishInterruptedResolution(record, false);
        }
        if (record.state() != BranchState.PREPARED) {
            throw new TransactionStateException(
                    "Oracle transaction branch is not prepared for detached resolution");
        }

        BranchState intent = commit
                ? BranchState.COMMITTING
                : BranchState.ROLLING_BACK;
        if (!journal.transition(identity, Set.of(BranchState.PREPARED), intent)) {
            return resolve(identity, commit);
        }
        BranchRecord intended = record.withState(intent);
        if (!recoveredXids().contains(record.xid())) {
            transition(intended, BranchState.HEURISTIC_HAZARD);
            return TransactionBranchState.TRANSACTION_BRANCH_STATE_HEURISTIC_HAZARD;
        }
        return resolveRecovered(intended, commit);
    }

    private TransactionBranchState finishInterruptedResolution(
            BranchRecord record, boolean commit) throws SQLException, XAException {
        if (!recoveredXids().contains(record.xid())) {
            BranchState terminal = commit
                    ? BranchState.COMMITTED
                    : BranchState.ROLLED_BACK;
            transition(record, terminal);
            return terminal.publicState();
        }
        return resolveRecovered(record, commit);
    }

    private TransactionBranchState resolveRecovered(BranchRecord record, boolean commit)
            throws SQLException, XAException {
        BranchState terminal = commit
                ? BranchState.COMMITTED
                : BranchState.ROLLED_BACK;
        try (XaSession session = sessionFactory.openSession()) {
            try {
                if (commit) {
                    session.xaResource().commit(record.xid(), false);
                } else {
                    session.xaResource().rollback(record.xid());
                }
                transition(record, terminal);
                return terminal.publicState();
            } catch (XAException exception) {
                BranchState heuristic = XaSupport.heuristicState(exception);
                if (heuristic != null) {
                    transition(record, heuristic);
                    return heuristic.publicState();
                }
                if (exception.errorCode == XAException.XAER_NOTA) {
                    Optional<BranchRecord> current = journal.find(record.identity());
                    if (current.isPresent() && current.orElseThrow().state().isTerminal()) {
                        return current.orElseThrow().state().publicState();
                    }
                    transition(record, terminal);
                    return terminal.publicState();
                }
                throw exception;
            }
        }
    }

    private Set<BranchXid> recoveredXids() throws SQLException, XAException {
        try (XaSession session = sessionFactory.openSession()) {
            return XaSupport.recover(session.xaResource());
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

    @Override
    public void close() throws Exception {
        lease.close();
    }
}
