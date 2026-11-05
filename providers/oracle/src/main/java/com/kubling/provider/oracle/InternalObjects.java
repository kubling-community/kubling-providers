package com.kubling.provider.oracle;

final class InternalObjects {
    private InternalObjects() {}

    static boolean isExposedRelation(String name) {
        return name != null
                && !JdbcTransactionJournal.TABLE_NAME.equalsIgnoreCase(name)
                && !JdbcResourceManagerLease.TABLE_NAME.equalsIgnoreCase(name);
    }
}
