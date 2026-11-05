package com.kubling.provider.oracle;

import com.kubling.provider.grpc.Assignment;
import com.kubling.provider.grpc.DeleteRequest;
import com.kubling.provider.grpc.Field;
import com.kubling.provider.grpc.InsertRequest;
import com.kubling.provider.grpc.Literal;
import com.kubling.provider.grpc.Tuple;
import com.kubling.provider.grpc.TupleBatch;
import com.kubling.provider.grpc.UpdateRequest;
import com.kubling.transport.grpc.TypeDescriptor;
import com.kubling.transport.grpc.ValueType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class MutationTranslator {
    private static final int MAX_FIELDS = 1_024;
    private static final int MAX_ROWS = 10_000;
    private static final int MAX_VALUES = 100_000;

    private final QueryTranslator expressions;

    MutationTranslator(SchemaPolicy schemaPolicy) {
        this.expressions = new QueryTranslator(schemaPolicy);
    }

    InsertPlan translate(InsertRequest request) {
        require(request.hasEntity(), "insert entity is required");
        require(request.getReturningFieldsCount() == 0,
                "Oracle generated or returning values are not supported");
        require(request.hasRows(), "insert rows are required");
        TupleBatch rows = request.getRows();
        require(rows.getFieldsCount() > 0, "insert fields are required");
        require(rows.getFieldsCount() <= MAX_FIELDS, "insert contains too many fields");
        require(rows.getTuplesCount() > 0, "insert tuples are required");
        require(rows.getTuplesCount() <= MAX_ROWS, "insert contains too many tuples");
        require((long) rows.getFieldsCount() * rows.getTuplesCount() <= MAX_VALUES,
                "insert contains too many values");

        List<Field> fields = rows.getFieldsList();
        List<TypeDescriptor> types = new ArrayList<>(fields.size());
        Set<String> names = new HashSet<>();
        StringBuilder sql = new StringBuilder("INSERT INTO ")
                .append(expressions.qualifiedEntity(request.getEntity()))
                .append(" (");
        for (int index = 0; index < fields.size(); index++) {
            Field field = fields.get(index);
            String name = QueryTranslator.requireIdentifier(
                    field.getName(), "insert field name");
            require(names.add(name), "insert field is repeated: " + name);
            types.add(fieldType(field));
            if (index > 0) {
                sql.append(", ");
            }
            sql.append(QueryTranslator.quoteIdentifier(name));
        }
        sql.append(") VALUES (");
        for (int index = 0; index < fields.size(); index++) {
            if (index > 0) {
                sql.append(", ");
            }
            sql.append('?');
        }
        sql.append(')');

        List<List<Literal>> parameters = new ArrayList<>(rows.getTuplesCount());
        for (int rowIndex = 0; rowIndex < rows.getTuplesCount(); rowIndex++) {
            Tuple tuple = rows.getTuples(rowIndex);
            require(tuple.getValuesCount() == fields.size(),
                    "insert tuple " + rowIndex + " has an invalid value count");
            List<Literal> row = new ArrayList<>(fields.size());
            for (int columnIndex = 0; columnIndex < fields.size(); columnIndex++) {
                row.add(Literal.newBuilder()
                        .setValue(tuple.getValues(columnIndex))
                        .setDeclaredType(types.get(columnIndex))
                        .build());
            }
            parameters.add(row);
        }
        return new InsertPlan(sql.toString(), parameters);
    }

    MutationPlan translate(UpdateRequest request) {
        require(request.hasEntity(), "update entity is required");
        require(request.getAssignmentsCount() > 0, "update assignments are required");
        require(request.getAssignmentsCount() <= MAX_FIELDS,
                "update contains too many assignments");

        List<Literal> parameters = new ArrayList<>();
        Set<String> fields = new HashSet<>();
        StringBuilder sql = new StringBuilder("UPDATE ")
                .append(expressions.qualifiedEntity(request.getEntity()))
                .append(" SET ");
        for (int index = 0; index < request.getAssignmentsCount(); index++) {
            Assignment assignment = request.getAssignments(index);
            String field = QueryTranslator.requireIdentifier(
                    assignment.getField(), "assignment field");
            require(fields.add(field), "assignment field is repeated: " + field);
            require(assignment.hasValue(), "assignment value is required");
            if (index > 0) {
                sql.append(", ");
            }
            sql.append(QueryTranslator.quoteIdentifier(field))
                    .append(" = ")
                    .append(expressions.renderAssignmentValue(
                            assignment.getValue(), parameters));
        }
        if (request.hasFilter()) {
            sql.append(" WHERE ")
                    .append(expressions.renderMutationPredicate(
                            request.getFilter(), parameters));
        }
        return new MutationPlan(sql.toString(), parameters);
    }

    MutationPlan translate(DeleteRequest request) {
        require(request.hasEntity(), "delete entity is required");
        List<Literal> parameters = new ArrayList<>();
        StringBuilder sql = new StringBuilder("DELETE FROM ")
                .append(expressions.qualifiedEntity(request.getEntity()));
        if (request.hasFilter()) {
            sql.append(" WHERE ")
                    .append(expressions.renderMutationPredicate(
                            request.getFilter(), parameters));
        }
        return new MutationPlan(sql.toString(), parameters);
    }

    private static TypeDescriptor fieldType(Field field) {
        TypeDescriptor descriptor = field.hasTypeDescriptor()
                ? field.getTypeDescriptor()
                : TypeDescriptor.newBuilder().setType(field.getType()).build();
        require(descriptor.getType() != ValueType.VALUE_TYPE_UNKNOWN,
                "insert field type is required: " + field.getName());
        require(JdbcValueCodec.supportedTypes().contains(descriptor.getType()),
                "insert field type is not supported: " + field.getName());
        require(field.getType() == ValueType.VALUE_TYPE_UNKNOWN
                        || field.getType() == descriptor.getType(),
                "insert field type does not match its descriptor: " + field.getName());
        return descriptor;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new RequestValidationException(message);
        }
    }
}
