package com.kubling.provider.oracle;

import com.kubling.provider.grpc.Literal;
import java.util.List;

record InsertPlan(String sql, List<List<Literal>> rows) {
    InsertPlan {
        sql = sql == null ? "" : sql;
        rows = rows == null
                ? List.of()
                : rows.stream().map(List::copyOf).toList();
    }
}
