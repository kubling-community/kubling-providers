package com.kubling.provider.oracle;

import com.kubling.provider.grpc.Literal;
import java.util.List;

record MutationPlan(String sql, List<Literal> parameters) {
    MutationPlan {
        sql = sql == null ? "" : sql;
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
    }
}
