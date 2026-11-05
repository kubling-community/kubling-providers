package com.kubling.provider.oracle;

import com.kubling.provider.grpc.AggregateCapabilities;
import com.kubling.provider.grpc.AggregateFunction;
import com.kubling.provider.grpc.ComparisonOperator;
import com.kubling.provider.grpc.DefaultNullOrder;
import com.kubling.provider.grpc.ExpressionCapabilities;
import com.kubling.provider.grpc.GetCapabilitiesResponse;
import com.kubling.provider.grpc.LogicalOperator;
import com.kubling.provider.grpc.MutationCapabilities;
import com.kubling.provider.grpc.NullPredicateOperator;
import com.kubling.provider.grpc.OrderingCapabilities;
import com.kubling.provider.grpc.PaginationCapabilities;
import com.kubling.provider.grpc.PatternOperator;
import com.kubling.provider.grpc.QueryCapabilities;
import com.kubling.provider.grpc.SupportedValueType;
import com.kubling.provider.grpc.TransactionCapabilities;
import com.kubling.provider.grpc.TransactionRecoveryCapabilities;
import com.kubling.provider.grpc.ValueCapabilities;

final class Capabilities {

    private Capabilities() {}

    static GetCapabilitiesResponse response() {
        return response(null);
    }

    static GetCapabilitiesResponse response(String resourceManagerId) {
        ValueCapabilities.Builder values = ValueCapabilities.newBuilder();
        JdbcValueCodec.supportedTypes().stream()
                .sorted()
                .forEach(type -> values.addSupportedTypes(SupportedValueType.newBuilder()
                        .setType(type)
                        .setInput(true)
                        .setOutput(true)));

        ExpressionCapabilities expressions = ExpressionCapabilities.newBuilder()
                .addComparisonOperators(ComparisonOperator.COMPARISON_OPERATOR_EQUAL)
                .addComparisonOperators(ComparisonOperator.COMPARISON_OPERATOR_NOT_EQUAL)
                .addComparisonOperators(ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN)
                .addComparisonOperators(ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN_OR_EQUAL)
                .addComparisonOperators(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN)
                .addComparisonOperators(ComparisonOperator.COMPARISON_OPERATOR_GREATER_THAN_OR_EQUAL)
                .addLogicalOperators(LogicalOperator.LOGICAL_OPERATOR_AND)
                .addLogicalOperators(LogicalOperator.LOGICAL_OPERATOR_OR)
                .addLogicalOperators(LogicalOperator.LOGICAL_OPERATOR_NOT)
                .addNullPredicateOperators(NullPredicateOperator.NULL_PREDICATE_OPERATOR_IS_NULL)
                .addNullPredicateOperators(
                        NullPredicateOperator.NULL_PREDICATE_OPERATOR_IS_NOT_NULL)
                .addPatternOperators(PatternOperator.PATTERN_OPERATOR_LIKE)
                .addPatternOperators(PatternOperator.PATTERN_OPERATOR_NOT_LIKE)
                .build();
        AggregateCapabilities aggregates = AggregateCapabilities.newBuilder()
                .addFunctions(AggregateFunction.AGGREGATE_FUNCTION_COUNT_STAR)
                .addFunctions(AggregateFunction.AGGREGATE_FUNCTION_COUNT)
                .addFunctions(AggregateFunction.AGGREGATE_FUNCTION_COUNT_BIG)
                .addFunctions(AggregateFunction.AGGREGATE_FUNCTION_MIN)
                .addFunctions(AggregateFunction.AGGREGATE_FUNCTION_MAX)
                .addFunctions(AggregateFunction.AGGREGATE_FUNCTION_SUM)
                .addFunctions(AggregateFunction.AGGREGATE_FUNCTION_AVG)
                .setDistinct(true)
                .setGroupBy(true)
                .setHaving(true)
                .build();
        QueryCapabilities query = QueryCapabilities.newBuilder()
                .setOrdering(OrderingCapabilities.newBuilder()
                        .setSupported(true)
                        .setExplicitNullOrdering(true)
                        .setDefaultNullOrder(DefaultNullOrder.DEFAULT_NULL_ORDER_HIGH))
                .setPagination(PaginationCapabilities.newBuilder()
                        .setLimit(true)
                        .setOffset(true))
                .setExpressions(expressions)
                .setAggregates(aggregates)
                .setPartialResults(false)
                .build();

        TransactionRecoveryCapabilities.Builder recovery =
                TransactionRecoveryCapabilities.newBuilder().setSupported(resourceManagerId != null);
        if (resourceManagerId != null) {
            recovery.setResourceManagerId(resourceManagerId);
        }

        return GetCapabilitiesResponse.newBuilder()
                .setTransactions(TransactionCapabilities.newBuilder()
                        .setSupported(true)
                        .setRecovery(recovery))
                .setQuery(query)
                .setMutations(MutationCapabilities.newBuilder()
                        .setInsert(true)
                        .setUpdate(true)
                        .setDelete(true)
                        .setGeneratedValues(false))
                .setValues(values)
                .build();
    }
}
