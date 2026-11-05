package com.kubling.provider.oracle;

import com.kubling.provider.grpc.Literal;
import java.util.List;

record QueryPlan(String sql, List<Literal> parameters, int batchSize) {
    QueryPlan {
        parameters = List.copyOf(parameters);
    }
}
