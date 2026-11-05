package com.kubling.provider.oracle;

import com.kubling.provider.grpc.SchemaMetadata;
import java.sql.SQLException;

@FunctionalInterface
interface SchemaDiscovery {
    SchemaMetadata discover() throws SQLException;
}
