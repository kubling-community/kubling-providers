package com.kubling.provider.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.kubling.provider.grpc.Assignment;
import com.kubling.provider.grpc.ComparisonExpression;
import com.kubling.provider.grpc.ComparisonOperator;
import com.kubling.provider.grpc.DeleteRequest;
import com.kubling.provider.grpc.EntityReference;
import com.kubling.provider.grpc.Expression;
import com.kubling.provider.grpc.Field;
import com.kubling.provider.grpc.FieldReference;
import com.kubling.provider.grpc.InsertRequest;
import com.kubling.provider.grpc.Literal;
import com.kubling.provider.grpc.Tuple;
import com.kubling.provider.grpc.TupleBatch;
import com.kubling.provider.grpc.UpdateRequest;
import com.kubling.transport.grpc.TypeDescriptor;
import com.kubling.transport.grpc.Value;
import com.kubling.transport.grpc.ValueType;
import java.util.Set;
import org.junit.jupiter.api.Test;

final class MutationTranslatorTest {
    private final MutationTranslator translator = new MutationTranslator(
            new SchemaPolicy(Set.of("KUBLING"), Set.of()));

    @Test
    void translatesMultiRowInsertWithTypedParameters() {
        InsertRequest request = InsertRequest.newBuilder()
                .setEntity(entity("KUBLING", "Order"))
                .setRows(TupleBatch.newBuilder()
                        .addFields(field("id", ValueType.VALUE_TYPE_LONG))
                        .addFields(field("select", ValueType.VALUE_TYPE_STRING))
                        .addTuples(Tuple.newBuilder()
                                .addValues(Value.newBuilder().setLongValue(7))
                                .addValues(Value.newBuilder().setStringValue("first")))
                        .addTuples(Tuple.newBuilder()
                                .addValues(Value.newBuilder().setLongValue(11))
                                .addValues(Value.newBuilder().setStringValue("second"))))
                .build();

        InsertPlan plan = translator.translate(request);

        assertEquals(
                "INSERT INTO \"KUBLING\".\"Order\" (\"id\", \"select\") VALUES (?, ?)",
                plan.sql());
        assertEquals(2, plan.rows().size());
        assertEquals(7, plan.rows().get(0).get(0).getValue().getLongValue());
        assertEquals("second", plan.rows().get(1).get(1).getValue().getStringValue());
        assertEquals(
                ValueType.VALUE_TYPE_STRING,
                plan.rows().get(1).get(1).getDeclaredType().getType());
    }

    @Test
    void translatesUpdateAndDeleteWithBoundPredicates() {
        UpdateRequest update = UpdateRequest.newBuilder()
                .setEntity(entity("KUBLING", "Order"))
                .addAssignments(Assignment.newBuilder()
                        .setField("Amount")
                        .setValue(decimalLiteral("19.95")))
                .setFilter(comparison(
                        ComparisonOperator.COMPARISON_OPERATOR_EQUAL,
                        fieldReference("select"),
                        stringLiteral("first")))
                .build();
        DeleteRequest delete = DeleteRequest.newBuilder()
                .setEntity(entity("KUBLING", "Order"))
                .setFilter(comparison(
                        ComparisonOperator.COMPARISON_OPERATOR_LESS_THAN,
                        fieldReference("id"),
                        longLiteral(10)))
                .build();

        MutationPlan updatePlan = translator.translate(update);
        MutationPlan deletePlan = translator.translate(delete);

        assertEquals(
                "UPDATE \"KUBLING\".\"Order\" SET \"Amount\" = ? "
                        + "WHERE (\"select\" = ?)",
                updatePlan.sql());
        assertEquals("19.95", updatePlan.parameters().get(0).getValue().getBigdecimalValue());
        assertEquals("first", updatePlan.parameters().get(1).getValue().getStringValue());
        assertEquals(
                "DELETE FROM \"KUBLING\".\"Order\" WHERE (\"id\" < ?)",
                deletePlan.sql());
        assertEquals(10, deletePlan.parameters().get(0).getValue().getLongValue());
    }

    @Test
    void quotesMutationIdentifiersAndDoesNotInterpolateValues() {
        String namespace = "APP\"; DROP USER APP; --";
        MutationTranslator unusualTranslator = new MutationTranslator(
                new SchemaPolicy(Set.of(namespace), Set.of()));
        String maliciousValue = "x' OR 1=1 --";
        UpdateRequest request = UpdateRequest.newBuilder()
                .setEntity(entity(namespace, "table\"name"))
                .addAssignments(Assignment.newBuilder()
                        .setField("column\"name")
                        .setValue(stringLiteral(maliciousValue)))
                .build();

        MutationPlan plan = unusualTranslator.translate(request);

        assertEquals(
                "UPDATE \"APP\"\"; DROP USER APP; --\".\"table\"\"name\" "
                        + "SET \"column\"\"name\" = ?",
                plan.sql());
        assertFalse(plan.sql().contains(maliciousValue));
        assertEquals(maliciousValue, plan.parameters().get(0).getValue().getStringValue());
    }

    @Test
    void rejectsUnsupportedMutationShapesBeforeJdbcExecution() {
        InsertRequest base = InsertRequest.newBuilder()
                .setEntity(entity("KUBLING", "Order"))
                .setRows(TupleBatch.newBuilder()
                        .addFields(field("id", ValueType.VALUE_TYPE_LONG))
                        .addTuples(Tuple.newBuilder()
                                .addValues(Value.newBuilder().setLongValue(1))))
                .build();

        assertThrows(
                RequestValidationException.class,
                () -> translator.translate(base.toBuilder().addReturningFields("id").build()));
        assertThrows(
                RequestValidationException.class,
                () -> translator.translate(base.toBuilder()
                        .setRows(base.getRows().toBuilder()
                                .addFields(field("id", ValueType.VALUE_TYPE_LONG)))
                        .build()));
        assertThrows(
                RequestValidationException.class,
                () -> translator.translate(base.toBuilder()
                        .setRows(base.getRows().toBuilder()
                                .setTuples(0, Tuple.getDefaultInstance()))
                        .build()));
        assertThrows(
                RequestValidationException.class,
                () -> translator.translate(UpdateRequest.newBuilder()
                        .setEntity(entity("KUBLING", "Order"))
                        .addAssignments(Assignment.newBuilder()
                                .setField("Amount")
                                .setValue(comparison(
                                        ComparisonOperator.COMPARISON_OPERATOR_EQUAL,
                                        fieldReference("id"),
                                        longLiteral(1))))
                        .build()));
        assertThrows(
                RequestValidationException.class,
                () -> translator.translate(DeleteRequest.newBuilder()
                        .setEntity(entity("SYS", "USER$"))
                        .build()));
    }

    private static EntityReference entity(String namespace, String name) {
        return EntityReference.newBuilder()
                .setNamespace(namespace)
                .setName(name)
                .build();
    }

    private static Field field(String name, ValueType type) {
        return Field.newBuilder()
                .setName(name)
                .setType(type)
                .setTypeDescriptor(TypeDescriptor.newBuilder().setType(type))
                .build();
    }

    private static Expression fieldReference(String name) {
        return Expression.newBuilder()
                .setField(FieldReference.newBuilder().setName(name))
                .build();
    }

    private static Expression comparison(
            ComparisonOperator operator,
            Expression left,
            Expression right) {
        return Expression.newBuilder()
                .setComparison(ComparisonExpression.newBuilder()
                        .setOperator(operator)
                        .setLeft(left)
                        .setRight(right))
                .build();
    }

    private static Expression stringLiteral(String value) {
        return literal(Value.newBuilder().setStringValue(value), ValueType.VALUE_TYPE_STRING);
    }

    private static Expression decimalLiteral(String value) {
        return literal(
                Value.newBuilder().setBigdecimalValue(value),
                ValueType.VALUE_TYPE_BIGDECIMAL);
    }

    private static Expression longLiteral(long value) {
        return literal(Value.newBuilder().setLongValue(value), ValueType.VALUE_TYPE_LONG);
    }

    private static Expression literal(Value.Builder value, ValueType type) {
        return Expression.newBuilder()
                .setLiteral(Literal.newBuilder()
                        .setValue(value)
                        .setDeclaredType(TypeDescriptor.newBuilder().setType(type)))
                .build();
    }
}
