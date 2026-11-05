package com.kubling.provider.oracle;

import com.google.protobuf.ByteString;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

final class JdbcTransactionJournal implements TransactionJournal {
    static final String TABLE_NAME = "KUBLING_PROVIDER_TX_BRANCHES";

    private static final String COLUMNS =
            "RESOURCE_MANAGER_ID, BRANCH_ID, FORMAT_ID, GLOBAL_ID, "
                    + "BRANCH_QUALIFIER, BRANCH_STATE, CONNECTION_ID";

    private final ConnectionFactory connectionFactory;
    private final String resourceManagerId;

    JdbcTransactionJournal(
            ConnectionFactory connectionFactory, String resourceManagerId) {
        this.connectionFactory = connectionFactory;
        this.resourceManagerId = resourceManagerId;
    }

    @Override
    public void validate() throws SQLException {
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT " + COLUMNS + " FROM " + TABLE_NAME + " WHERE 1 = 0")) {
            statement.executeQuery().close();
        }
    }

    @Override
    public boolean insert(BranchRecord record) throws SQLException {
        String sql = "INSERT INTO " + TABLE_NAME + " (" + COLUMNS
                + ", UPDATED_AT) VALUES (?, ?, ?, ?, ?, ?, ?, SYSTIMESTAMP)";
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            bindRecord(statement, record);
            return statement.executeUpdate() == 1;
        } catch (SQLException exception) {
            if (exception.getErrorCode() == 1) {
                return false;
            }
            throw exception;
        }
    }

    @Override
    public Optional<BranchRecord> find(BranchIdentity identity) throws SQLException {
        String sql = "SELECT " + COLUMNS + " FROM " + TABLE_NAME
                + " WHERE RESOURCE_MANAGER_ID = ? AND BRANCH_ID = ?";
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, identity.resourceManagerId());
            statement.setBytes(2, identity.branchId().toByteArray());
            try (ResultSet results = statement.executeQuery()) {
                return results.next() ? Optional.of(read(results)) : Optional.empty();
            }
        }
    }

    @Override
    public List<BranchRecord> pending() throws SQLException {
        String sql = "SELECT " + COLUMNS + " FROM " + TABLE_NAME
                + " WHERE RESOURCE_MANAGER_ID = ?"
                + " AND BRANCH_STATE NOT IN ('READ_ONLY', 'COMMITTED', 'ROLLED_BACK')"
                + " ORDER BY UPDATED_AT, BRANCH_ID";
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, resourceManagerId);
            try (ResultSet results = statement.executeQuery()) {
                List<BranchRecord> records = new ArrayList<>();
                while (results.next()) {
                    records.add(read(results));
                }
                return List.copyOf(records);
            }
        }
    }

    @Override
    public boolean transition(
            BranchIdentity identity,
            Set<BranchState> expected,
            BranchState nextState)
            throws SQLException {
        if (expected.isEmpty()) {
            throw new IllegalArgumentException("expected transaction states must not be empty");
        }
        String placeholders = String.join(", ", expected.stream().map(ignored -> "?").toList());
        String sql = "UPDATE " + TABLE_NAME
                + " SET BRANCH_STATE = ?, UPDATED_AT = SYSTIMESTAMP"
                + " WHERE RESOURCE_MANAGER_ID = ? AND BRANCH_ID = ?"
                + " AND BRANCH_STATE IN (" + placeholders + ")";
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            statement.setString(parameter++, nextState.name());
            statement.setString(parameter++, identity.resourceManagerId());
            statement.setBytes(parameter++, identity.branchId().toByteArray());
            for (BranchState state : expected) {
                statement.setString(parameter++, state.name());
            }
            return statement.executeUpdate() == 1;
        }
    }

    @Override
    public boolean delete(BranchIdentity identity) throws SQLException {
        String sql = "DELETE FROM " + TABLE_NAME
                + " WHERE RESOURCE_MANAGER_ID = ? AND BRANCH_ID = ?";
        try (Connection connection = openConnection();
                PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, identity.resourceManagerId());
            statement.setBytes(2, identity.branchId().toByteArray());
            return statement.executeUpdate() == 1;
        }
    }

    private static void bindRecord(PreparedStatement statement, BranchRecord record)
            throws SQLException {
        statement.setString(1, record.identity().resourceManagerId());
        statement.setBytes(2, record.identity().branchId().toByteArray());
        statement.setInt(3, record.xid().getFormatId());
        statement.setBytes(4, record.xid().getGlobalTransactionId());
        statement.setBytes(5, record.xid().getBranchQualifier());
        statement.setString(6, record.state().name());
        statement.setString(7, record.connectionId());
    }

    private Connection openConnection() throws SQLException {
        Connection connection = connectionFactory.openConnection();
        try {
            if (!connection.getAutoCommit()) {
                connection.setAutoCommit(true);
            }
            return connection;
        } catch (SQLException exception) {
            try {
                connection.close();
            } catch (SQLException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }

    private static BranchRecord read(ResultSet results) throws SQLException {
        BranchIdentity identity = new BranchIdentity(
                results.getString("RESOURCE_MANAGER_ID"),
                ByteString.copyFrom(results.getBytes("BRANCH_ID")));
        BranchXid xid = new BranchXid(
                results.getInt("FORMAT_ID"),
                results.getBytes("GLOBAL_ID"),
                results.getBytes("BRANCH_QUALIFIER"));
        BranchState state = BranchState.valueOf(results.getString("BRANCH_STATE"));
        return new BranchRecord(identity, xid, state, results.getString("CONNECTION_ID"));
    }
}
