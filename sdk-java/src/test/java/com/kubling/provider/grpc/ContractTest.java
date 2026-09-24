package com.kubling.provider.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kubling.transport.grpc.ArrayValue;
import com.kubling.transport.grpc.NullValue;
import com.kubling.transport.grpc.TypeDescriptor;
import com.kubling.transport.grpc.Value;
import com.kubling.transport.grpc.ValueType;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.Test;

class ContractTest {
  @Test
  void preservesSharedTypedValuesWithoutRegeneratingThem() throws Exception {
    TypeDescriptor integerType = TypeDescriptor.newBuilder()
        .setType(ValueType.VALUE_TYPE_INTEGER)
        .build();
    TypeDescriptor arrayType = TypeDescriptor.newBuilder()
        .setType(ValueType.VALUE_TYPE_ARRAY)
        .setElementType(integerType)
        .build();
    Value value = Value.newBuilder()
        .setArrayValue(ArrayValue.newBuilder()
            .setElementType(integerType)
            .addElements(Value.newBuilder().setIntegerValue(7))
            .addElements(Value.newBuilder().setNullValue(NullValue.getDefaultInstance())))
        .build();
    Field field = Field.newBuilder()
        .setName("numbers")
        .setType(ValueType.VALUE_TYPE_ARRAY)
        .setTypeDescriptor(arrayType)
        .build();
    TupleBatch batch = TupleBatch.newBuilder()
        .addFields(field)
        .addTuples(Tuple.newBuilder().addValues(value))
        .build();

    TupleBatch decoded = TupleBatch.parseFrom(batch.toByteArray());
    assertEquals(arrayType, decoded.getFields(0).getTypeDescriptor());
    assertEquals(Value.KindCase.ARRAY_VALUE, decoded.getTuples(0).getValues(0).getKindCase());
  }

  @Test
  void preservesDeclaredNullAndValueCapabilities() throws Exception {
    Literal literal = Literal.newBuilder()
        .setValue(Value.newBuilder().setNullValue(NullValue.getDefaultInstance()))
        .setDeclaredType(TypeDescriptor.newBuilder().setType(ValueType.VALUE_TYPE_CLOB))
        .build();
    assertTrue(Literal.parseFrom(literal.toByteArray()).hasDeclaredType());

    ValueCapabilities capabilities = ValueCapabilities.newBuilder()
        .addFeatures("array_values_v1")
        .setMaxArrayDimensions(2)
        .build();
    assertTrue(ValueCapabilities.parseFrom(capabilities.toByteArray()).hasMaxArrayDimensions());
  }

  @Test
  void preservesAggregatePushdownContract() throws Exception {
    AggregateCall aggregate = AggregateCall.newBuilder()
        .setFunction(AggregateFunction.AGGREGATE_FUNCTION_AVG)
        .addArguments(Expression.newBuilder()
            .setField(FieldReference.newBuilder().setName("amount")))
        .setResultType(TypeDescriptor.newBuilder().setType(ValueType.VALUE_TYPE_DOUBLE))
        .build();
    QueryRequest request = QueryRequest.newBuilder()
        .addProjections(Projection.newBuilder()
            .setExpression(Expression.newBuilder().setAggregate(aggregate))
            .setOutputName("average_amount"))
        .addGroupBy(Expression.newBuilder()
            .setField(FieldReference.newBuilder().setName("category")))
        .setHaving(Expression.newBuilder()
            .setLiteral(Literal.newBuilder()
                .setValue(Value.newBuilder().setBooleanValue(true))))
        .build();

    QueryRequest decoded = QueryRequest.parseFrom(request.toByteArray());
    assertEquals(
        AggregateFunction.AGGREGATE_FUNCTION_AVG,
        decoded.getProjections(0).getExpression().getAggregate().getFunction());
    assertEquals(ValueType.VALUE_TYPE_DOUBLE,
        decoded.getProjections(0).getExpression().getAggregate().getResultType().getType());
    assertEquals("category", decoded.getGroupBy(0).getField().getName());
    assertTrue(decoded.hasHaving());

    AggregateCapabilities capabilities = AggregateCapabilities.newBuilder()
        .addFunctions(AggregateFunction.AGGREGATE_FUNCTION_AVG)
        .setDistinct(true)
        .setGroupBy(true)
        .setHaving(true)
        .build();
    assertEquals(
        AggregateFunction.AGGREGATE_FUNCTION_AVG,
        AggregateCapabilities.parseFrom(capabilities.toByteArray()).getFunctions(0));
  }

  @Test
  void exposesProviderAndLobServiceDescriptors() {
    assertEquals(
        MethodDescriptor.MethodType.SERVER_STREAMING,
        ProviderServiceGrpc.getQueryMethod().getType());
    assertEquals(
        MethodDescriptor.MethodType.SERVER_STREAMING,
        ProviderServiceGrpc.getReadLobMethod().getType());
    assertEquals(
        MethodDescriptor.MethodType.UNARY,
        ProviderServiceGrpc.getReleaseLobMethod().getType());
    assertNotNull(getClass().getResource("/META-INF/proto/kubling/provider/v1/provider.proto"));
  }
}
