package com.kubling.provider.oracle;

import java.sql.SQLException;

final class XaLogicalConnectionFactory implements LogicalConnectionFactory {
    private final XaSessionFactory sessionFactory;
    private final String resourceManagerId;
    private final TransactionJournal journal;

    XaLogicalConnectionFactory(
            XaSessionFactory sessionFactory,
            String resourceManagerId,
            TransactionJournal journal) {
        this.sessionFactory = sessionFactory;
        this.resourceManagerId = resourceManagerId;
        this.journal = journal;
    }

    @Override
    public LogicalConnection open(String connectionId) throws SQLException {
        XaSession session = sessionFactory.openSession();
        try {
            return LogicalConnection.xa(
                    connectionId, session, resourceManagerId, journal);
        } catch (SQLException | RuntimeException exception) {
            try {
                session.close();
            } catch (SQLException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }
}
