package com.kubling.provider.oracle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kubling.provider.grpc.ColumnSearchability;
import com.kubling.transport.grpc.ValueType;
import java.sql.Types;
import org.junit.jupiter.api.Test;

final class TypeMapperTest {
    @Test
    void mapsOracleNumbersWithoutNarrowingTheirRange() {
        TypeMapper.Mapping integer = TypeMapper.map(
                        Types.NUMERIC,
                        "NUMBER",
                        20,
                        0)
                .orElseThrow();
        TypeMapper.Mapping decimal = TypeMapper.map(
                        Types.NUMERIC,
                        "NUMBER",
                        18,
                        4)
                .orElseThrow();
        TypeMapper.Mapping unconstrained = TypeMapper.map(
                        Types.NUMERIC,
                        "NUMBER",
                        38,
                        -127)
                .orElseThrow();

        assertEquals(ValueType.VALUE_TYPE_BIGINTEGER, integer.type());
        assertEquals(20, integer.precision());
        assertEquals(0, integer.scale());
        assertEquals(ValueType.VALUE_TYPE_BIGDECIMAL, decimal.type());
        assertEquals(18, decimal.precision());
        assertEquals(4, decimal.scale());
        assertEquals(ValueType.VALUE_TYPE_BIGDECIMAL, unconstrained.type());
        assertEquals(38, unconstrained.precision());
        assertEquals(null, unconstrained.scale());
    }

    @Test
    void mapsOracleDateAndPlainTimestampWithoutLosingTheirTimeComponent() {
        assertEquals(
                ValueType.VALUE_TYPE_TIMESTAMP,
                TypeMapper.map(Types.DATE, "DATE", null, null).orElseThrow().type());
        assertEquals(
                ValueType.VALUE_TYPE_TIMESTAMP,
                TypeMapper.map(Types.TIMESTAMP, "TIMESTAMP(6)", null, 6)
                        .orElseThrow()
                        .type());
        assertFalse(TypeMapper.map(
                        Types.TIMESTAMP_WITH_TIMEZONE,
                        "TIMESTAMP(6) WITH TIME ZONE",
                        null,
                        6)
                .isPresent());
    }

    @Test
    void keepsOracleFloatArbitraryPrecisionSeparateFromBinaryFloatingPoint() {
        assertEquals(
                ValueType.VALUE_TYPE_BIGDECIMAL,
                TypeMapper.map(Types.FLOAT, "FLOAT", 126, -127)
                        .orElseThrow()
                        .type());
        assertEquals(
                ValueType.VALUE_TYPE_FLOAT,
                TypeMapper.map(Types.FLOAT, "BINARY_FLOAT", null, null)
                        .orElseThrow()
                        .type());
        assertEquals(
                ValueType.VALUE_TYPE_DOUBLE,
                TypeMapper.map(Types.DOUBLE, "BINARY_DOUBLE", null, null)
                        .orElseThrow()
                        .type());
    }

    @Test
    void mapsSupportedScalarAndLobTypesAndRejectsObjectTypes() {
        assertEquals(
                ValueType.VALUE_TYPE_STRING,
                TypeMapper.map(Types.VARCHAR, "VARCHAR2", 128, null)
                        .orElseThrow()
                        .type());
        assertEquals(
                128,
                TypeMapper.map(Types.VARCHAR, "VARCHAR2", 128, null)
                        .orElseThrow()
                        .length());
        assertEquals(
                ValueType.VALUE_TYPE_VARBINARY,
                TypeMapper.map(Types.VARBINARY, "RAW", 32, null)
                        .orElseThrow()
                        .type());
        TypeMapper.Mapping clob = TypeMapper.map(
                        Types.CLOB,
                        "CLOB",
                        null,
                        null)
                .orElseThrow();
        assertEquals(ValueType.VALUE_TYPE_CLOB, clob.type());
        assertEquals(
                ColumnSearchability.COLUMN_SEARCHABILITY_UNSEARCHABLE,
                clob.searchability());
        assertTrue(TypeMapper.map(
                        Types.STRUCT,
                        "MDSYS.SDO_GEOMETRY",
                        null,
                        null)
                .isEmpty());
    }
}
