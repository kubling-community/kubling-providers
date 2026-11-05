package com.kubling.provider.oracle;

import java.util.Set;

final class SchemaPolicy {
    private static final Set<String> DEFAULT_DENIED_SCHEMAS = Set.of(
            "ANONYMOUS",
            "APEX_PUBLIC_USER",
            "APPQOSSYS",
            "AUDSYS",
            "CTXSYS",
            "DBSNMP",
            "DGPDB_INT",
            "DIP",
            "DVF",
            "DVSYS",
            "FLOWS_FILES",
            "GGSYS",
            "GSMADMIN_INTERNAL",
            "GSMCATUSER",
            "GSMROOTUSER",
            "GSMUSER",
            "LBACSYS",
            "MDDATA",
            "MDSYS",
            "OJVMSYS",
            "OLAPSYS",
            "ORACLE_OCM",
            "ORDDATA",
            "ORDPLUGINS",
            "ORDSYS",
            "OUTLN",
            "PDBADMIN",
            "REMOTE_SCHEDULER_AGENT",
            "SI_INFORMTN_SCHEMA",
            "SPATIAL_CSW_ADMIN_USR",
            "SPATIAL_WFS_ADMIN_USR",
            "SYS",
            "SYSTEM",
            "SYSBACKUP",
            "SYSDG",
            "SYSKM",
            "SYSRAC",
            "WMSYS",
            "XS$NULL",
            "XDB");

    private final Set<String> allowlist;
    private final Set<String> denylist;

    SchemaPolicy(Set<String> allowlist, Set<String> denylist) {
        this.allowlist = Set.copyOf(allowlist);
        this.denylist = Set.copyOf(denylist);
    }

    boolean includes(String schema) {
        if (schema == null || schema.isEmpty() || denylist.contains(schema)) {
            return false;
        }
        if (!allowlist.isEmpty()) {
            return allowlist.contains(schema);
        }
        return !DEFAULT_DENIED_SCHEMAS.contains(schema) && !schema.startsWith("APEX_");
    }
}
