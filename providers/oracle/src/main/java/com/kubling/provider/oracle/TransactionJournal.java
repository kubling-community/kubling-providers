package com.kubling.provider.oracle;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

interface TransactionJournal {
    void validate() throws SQLException;

    boolean insert(BranchRecord record) throws SQLException;

    Optional<BranchRecord> find(BranchIdentity identity) throws SQLException;

    List<BranchRecord> pending() throws SQLException;

    boolean transition(
            BranchIdentity identity,
            Set<BranchState> expected,
            BranchState nextState)
            throws SQLException;

    boolean delete(BranchIdentity identity) throws SQLException;
}
