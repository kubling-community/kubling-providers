package com.kubling.provider.oracle;

import com.kubling.provider.grpc.ColumnSearchability;
import com.kubling.transport.grpc.ValueType;
import java.sql.Types;
import java.util.Locale;
import java.util.Optional;

final class TypeMapper {
    private TypeMapper() {}

    static Optional<Mapping> map(
            int jdbcType,
            String nativeType,
            Integer columnSize,
            Integer decimalDigits) {
        String normalizedType = nativeType == null
                ? ""
                : nativeType.strip().toUpperCase(Locale.ROOT);

        if (isTimeZoneAwareTimestamp(normalizedType)) {
            return Optional.empty();
        }
        if (normalizedType.equals("NUMBER")
                || normalizedType.equals("NUMERIC")
                || normalizedType.equals("DECIMAL")) {
            return Optional.of(number(columnSize, decimalDigits));
        }
        if (normalizedType.equals("BINARY_FLOAT")) {
            return Optional.of(simple(ValueType.VALUE_TYPE_FLOAT));
        }
        if (normalizedType.equals("BINARY_DOUBLE")) {
            return Optional.of(simple(ValueType.VALUE_TYPE_DOUBLE));
        }
        if (normalizedType.equals("FLOAT")) {
            return Optional.of(new Mapping(
                    ValueType.VALUE_TYPE_BIGDECIMAL,
                    null,
                    null,
                    null,
                    ColumnSearchability.COLUMN_SEARCHABILITY_ALL));
        }
        if (normalizedType.equals("DATE") || isPlainTimestamp(normalizedType)) {
            return Optional.of(simple(ValueType.VALUE_TYPE_TIMESTAMP));
        }
        if (normalizedType.equals("BOOLEAN")) {
            return Optional.of(simple(ValueType.VALUE_TYPE_BOOLEAN));
        }
        if (isCharacterType(normalizedType)) {
            return Optional.of(withLength(ValueType.VALUE_TYPE_STRING, columnSize));
        }
        if (normalizedType.equals("LONG")) {
            return Optional.of(unsearchable(ValueType.VALUE_TYPE_CLOB));
        }
        if (normalizedType.equals("CLOB") || normalizedType.equals("NCLOB")) {
            return Optional.of(unsearchable(ValueType.VALUE_TYPE_CLOB));
        }
        if (normalizedType.equals("BLOB")) {
            return Optional.of(unsearchable(ValueType.VALUE_TYPE_BLOB));
        }
        if (normalizedType.equals("RAW") || normalizedType.equals("LONG RAW")) {
            return Optional.of(withLength(ValueType.VALUE_TYPE_VARBINARY, columnSize));
        }
        if (normalizedType.equals("ROWID") || normalizedType.equals("UROWID")) {
            return Optional.of(withLength(ValueType.VALUE_TYPE_STRING, columnSize));
        }
        return switch (jdbcType) {
            case Types.BOOLEAN, Types.BIT -> Optional.of(simple(ValueType.VALUE_TYPE_BOOLEAN));
            case Types.TINYINT -> Optional.of(simple(ValueType.VALUE_TYPE_BYTE));
            case Types.SMALLINT -> Optional.of(simple(ValueType.VALUE_TYPE_SHORT));
            case Types.INTEGER -> Optional.of(simple(ValueType.VALUE_TYPE_INTEGER));
            case Types.BIGINT -> Optional.of(simple(ValueType.VALUE_TYPE_LONG));
            case Types.NUMERIC, Types.DECIMAL -> Optional.of(number(columnSize, decimalDigits));
            case Types.REAL -> Optional.of(simple(ValueType.VALUE_TYPE_FLOAT));
            case Types.FLOAT, Types.DOUBLE -> Optional.of(simple(ValueType.VALUE_TYPE_DOUBLE));
            case Types.CHAR, Types.NCHAR, Types.VARCHAR, Types.NVARCHAR ->
                    Optional.of(withLength(ValueType.VALUE_TYPE_STRING, columnSize));
            case Types.LONGVARCHAR, Types.LONGNVARCHAR ->
                    Optional.of(unsearchable(ValueType.VALUE_TYPE_CLOB));
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY ->
                    Optional.of(withLength(ValueType.VALUE_TYPE_VARBINARY, columnSize));
            case Types.DATE -> Optional.of(simple(ValueType.VALUE_TYPE_DATE));
            case Types.TIME -> Optional.of(simple(ValueType.VALUE_TYPE_TIME));
            case Types.TIMESTAMP -> Optional.of(simple(ValueType.VALUE_TYPE_TIMESTAMP));
            case Types.BLOB -> Optional.of(unsearchable(ValueType.VALUE_TYPE_BLOB));
            case Types.CLOB, Types.NCLOB -> Optional.of(unsearchable(ValueType.VALUE_TYPE_CLOB));
            case Types.ROWID -> Optional.of(withLength(ValueType.VALUE_TYPE_STRING, columnSize));
            default -> Optional.empty();
        };
    }

    private static boolean isTimeZoneAwareTimestamp(String nativeType) {
        return nativeType.startsWith("TIMESTAMP") && nativeType.contains("WITH TIME ZONE")
                || nativeType.startsWith("TIMESTAMP")
                        && nativeType.contains("WITH LOCAL TIME ZONE");
    }

    private static boolean isPlainTimestamp(String nativeType) {
        return nativeType.startsWith("TIMESTAMP") && !nativeType.contains("WITH");
    }

    private static boolean isCharacterType(String nativeType) {
        return nativeType.equals("CHAR")
                || nativeType.equals("NCHAR")
                || nativeType.equals("VARCHAR")
                || nativeType.equals("VARCHAR2")
                || nativeType.equals("NVARCHAR2");
    }

    private static Mapping number(Integer precision, Integer scale) {
        Integer normalizedPrecision = positive(precision);
        Integer normalizedScale = scale;
        if (Integer.valueOf(-127).equals(normalizedScale)) {
            normalizedScale = null;
        }
        ValueType type = Integer.valueOf(0).equals(normalizedScale)
                ? ValueType.VALUE_TYPE_BIGINTEGER
                : ValueType.VALUE_TYPE_BIGDECIMAL;
        return new Mapping(
                type,
                null,
                normalizedPrecision,
                normalizedScale,
                ColumnSearchability.COLUMN_SEARCHABILITY_ALL);
    }

    private static Mapping simple(ValueType type) {
        return new Mapping(
                type,
                null,
                null,
                null,
                ColumnSearchability.COLUMN_SEARCHABILITY_ALL);
    }

    private static Mapping withLength(ValueType type, Integer length) {
        return new Mapping(
                type,
                positive(length),
                null,
                null,
                ColumnSearchability.COLUMN_SEARCHABILITY_ALL);
    }

    private static Mapping unsearchable(ValueType type) {
        return new Mapping(
                type,
                null,
                null,
                null,
                ColumnSearchability.COLUMN_SEARCHABILITY_UNSEARCHABLE);
    }

    private static Integer positive(Integer value) {
        return value != null && value > 0 ? value : null;
    }

    record Mapping(
            ValueType type,
            Integer length,
            Integer precision,
            Integer scale,
            ColumnSearchability searchability) {}
}
