package com.kubling.provider.oracle;

import com.kubling.provider.grpc.TransactionBranchState;

enum BranchState {
    ACTIVE,
    PREPARING,
    PREPARED,
    READ_ONLY,
    COMMITTING,
    COMMITTING_ONE_PHASE,
    COMMITTED,
    ROLLING_BACK,
    ROLLING_BACK_ACTIVE,
    ROLLED_BACK,
    HEURISTIC_COMMIT,
    HEURISTIC_ROLLBACK,
    HEURISTIC_MIXED,
    HEURISTIC_HAZARD;

    TransactionBranchState publicState() {
        return switch (this) {
            case ACTIVE -> TransactionBranchState.TRANSACTION_BRANCH_STATE_ACTIVE;
            case PREPARED -> TransactionBranchState.TRANSACTION_BRANCH_STATE_PREPARED;
            case READ_ONLY -> TransactionBranchState.TRANSACTION_BRANCH_STATE_READ_ONLY;
            case COMMITTED -> TransactionBranchState.TRANSACTION_BRANCH_STATE_COMMITTED;
            case ROLLED_BACK -> TransactionBranchState.TRANSACTION_BRANCH_STATE_ROLLED_BACK;
            case PREPARING,
                    COMMITTING,
                    COMMITTING_ONE_PHASE,
                    ROLLING_BACK,
                    ROLLING_BACK_ACTIVE ->
                TransactionBranchState.TRANSACTION_BRANCH_STATE_HEURISTIC_HAZARD;
            case HEURISTIC_COMMIT ->
                TransactionBranchState.TRANSACTION_BRANCH_STATE_HEURISTIC_COMMIT;
            case HEURISTIC_ROLLBACK ->
                TransactionBranchState.TRANSACTION_BRANCH_STATE_HEURISTIC_ROLLBACK;
            case HEURISTIC_MIXED ->
                TransactionBranchState.TRANSACTION_BRANCH_STATE_HEURISTIC_MIXED;
            case HEURISTIC_HAZARD ->
                TransactionBranchState.TRANSACTION_BRANCH_STATE_HEURISTIC_HAZARD;
        };
    }

    boolean isTerminal() {
        return switch (this) {
            case READ_ONLY,
                    COMMITTED,
                    ROLLED_BACK,
                    HEURISTIC_COMMIT,
                    HEURISTIC_ROLLBACK,
                    HEURISTIC_MIXED,
                    HEURISTIC_HAZARD -> true;
            default -> false;
        };
    }

    boolean isRecoverable() {
        return this == PREPARING
                || this == PREPARED
                || this == COMMITTING
                || this == COMMITTING_ONE_PHASE
                || this == ROLLING_BACK
                || this == ROLLING_BACK_ACTIVE
                || this == HEURISTIC_COMMIT
                || this == HEURISTIC_ROLLBACK
                || this == HEURISTIC_MIXED
                || this == HEURISTIC_HAZARD;
    }
}
