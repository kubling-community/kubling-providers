package com.kubling.provider.oracle;

import com.google.protobuf.ByteString;
import com.kubling.provider.grpc.Expression;
import com.kubling.provider.grpc.Field;
import com.kubling.provider.grpc.Literal;
import com.kubling.provider.grpc.Projection;
import com.kubling.transport.grpc.BlobValue;
import com.kubling.transport.grpc.ClobValue;
import com.kubling.transport.grpc.NullValue;
import com.kubling.transport.grpc.TypeDescriptor;
import com.kubling.transport.grpc.Value;
import com.kubling.transport.grpc.ValueType;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.EnumSet;
import java.util.Set;

final class JdbcValueCodec {
    private static final int MAX_INLINE_LOB_BYTES = 16 * 1024 * 1024;
    private static final Set<ValueType> SUPPORTED_TYPES = Set.copyOf(EnumSet.of(
            ValueType.VALUE_TYPE_STRING,
            ValueType.VALUE_TYPE_VARBINARY,
            ValueType.VALUE_TYPE_CHAR,
            ValueType.VALUE_TYPE_BOOLEAN,
            ValueType.VALUE_TYPE_BYTE,
            ValueType.VALUE_TYPE_SHORT,
            ValueType.VALUE_TYPE_INTEGER,
            ValueType.VALUE_TYPE_LONG,
            ValueType.VALUE_TYPE_BIGINTEGER,
            ValueType.VALUE_TYPE_FLOAT,
            ValueType.VALUE_TYPE_DOUBLE,
            ValueType.VALUE_TYPE_BIGDECIMAL,
            ValueType.VALUE_TYPE_DATE,
            ValueType.VALUE_TYPE_TIME,
            ValueType.VALUE_TYPE_TIMESTAMP,
            ValueType.VALUE_TYPE_BLOB,
            ValueType.VALUE_TYPE_CLOB));

    private JdbcValueCodec() {}

    static Set<ValueType> supportedTypes() {
        return SUPPORTED_TYPES;
    }

    static void bind(PreparedStatement statement, int index, Literal literal)
            throws SQLException {
        if (!literal.hasValue()) {
            throw invalid("literal value is required");
        }
        Value value = literal.getValue();
        ValueType actualType = valueType(value);
        ValueType declaredType = literal.hasDeclaredType()
                ? literal.getDeclaredType().getType()
                : ValueType.VALUE_TYPE_UNKNOWN;
        if (value.getKindCase() == Value.KindCase.NULL_VALUE) {
            if (declaredType == ValueType.VALUE_TYPE_UNKNOWN) {
                throw invalid("null literal requires a declared type");
            }
            statement.setNull(index, jdbcType(declaredType));
            return;
        }
        if (value.getKindCase() == Value.KindCase.LOB_REFERENCE) {
            throw invalid("LOB reference literals are not supported");
        }
        if (declaredType != ValueType.VALUE_TYPE_UNKNOWN && declaredType != actualType) {
            throw invalid("literal value does not match its declared type");
        }
        requireSupported(actualType);

        try {
            switch (actualType) {
                case VALUE_TYPE_STRING -> statement.setString(index, value.getStringValue());
                case VALUE_TYPE_VARBINARY ->
                        statement.setBytes(index, value.getVarbinaryValue().toByteArray());
                case VALUE_TYPE_CHAR -> {
                    String character = value.getCharValue();
                    if (character.codePointCount(0, character.length()) != 1
                            || character.codePointAt(0) > Character.MAX_VALUE) {
                        throw invalid("character literal is invalid");
                    }
                    statement.setString(index, character);
                }
                case VALUE_TYPE_BOOLEAN -> statement.setBoolean(index, value.getBooleanValue());
                case VALUE_TYPE_BYTE -> statement.setByte(
                        index,
                        BigDecimal.valueOf(value.getByteValue()).byteValueExact());
                case VALUE_TYPE_SHORT -> statement.setShort(
                        index,
                        BigDecimal.valueOf(value.getShortValue()).shortValueExact());
                case VALUE_TYPE_INTEGER -> statement.setInt(index, value.getIntegerValue());
                case VALUE_TYPE_LONG -> statement.setLong(index, value.getLongValue());
                case VALUE_TYPE_BIGINTEGER -> statement.setBigDecimal(
                        index,
                        new BigDecimal(new BigInteger(value.getBigintegerValue())));
                case VALUE_TYPE_FLOAT -> statement.setFloat(index, value.getFloatValue());
                case VALUE_TYPE_DOUBLE -> statement.setDouble(index, value.getDoubleValue());
                case VALUE_TYPE_BIGDECIMAL ->
                        statement.setBigDecimal(index, new BigDecimal(value.getBigdecimalValue()));
                case VALUE_TYPE_DATE ->
                        statement.setDate(index, Date.valueOf(LocalDate.parse(value.getDateValue())));
                case VALUE_TYPE_TIME ->
                        statement.setTime(index, Time.valueOf(LocalTime.parse(value.getTimeValue())));
                case VALUE_TYPE_TIMESTAMP -> statement.setTimestamp(
                        index,
                        Timestamp.valueOf(LocalDateTime.parse(value.getTimestampValue())));
                case VALUE_TYPE_BLOB -> statement.setBytes(
                        index,
                        value.getBlobValue().getData().toByteArray());
                case VALUE_TYPE_CLOB ->
                        statement.setString(index, value.getClobValue().getData());
                default -> throw invalid("literal type is not supported");
            }
        } catch (ArithmeticException | DateTimeParseException | NumberFormatException exception) {
            throw invalid("literal value is invalid", exception);
        }
    }

    static ResultColumn resultColumn(
            Projection projection,
            ResultSetMetaData metadata,
            int index) throws SQLException {
        TypeDescriptor descriptor = requestedType(projection.getExpression());
        if (descriptor == null) {
            descriptor = metadataType(metadata, index);
        }
        requireSupported(descriptor.getType());
        Field field = Field.newBuilder()
                .setName(projection.getOutputName())
                .setType(descriptor.getType())
                .setTypeDescriptor(descriptor)
                .build();
        return new ResultColumn(field, descriptor.getType());
    }

    static Value read(ResultSet resultSet, int index, ValueType type) throws SQLException {
        return switch (type) {
            case VALUE_TYPE_STRING -> nullableString(resultSet.getString(index), false);
            case VALUE_TYPE_VARBINARY -> nullableBytes(resultSet.getBytes(index));
            case VALUE_TYPE_CHAR -> nullableString(resultSet.getString(index), true);
            case VALUE_TYPE_BOOLEAN -> {
                boolean value = resultSet.getBoolean(index);
                yield resultSet.wasNull()
                        ? nullValue()
                        : Value.newBuilder().setBooleanValue(value).build();
            }
            case VALUE_TYPE_BYTE -> {
                BigDecimal value = resultSet.getBigDecimal(index);
                yield value == null
                        ? nullValue()
                        : Value.newBuilder().setByteValue(value.byteValueExact()).build();
            }
            case VALUE_TYPE_SHORT -> {
                BigDecimal value = resultSet.getBigDecimal(index);
                yield value == null
                        ? nullValue()
                        : Value.newBuilder().setShortValue(value.shortValueExact()).build();
            }
            case VALUE_TYPE_INTEGER -> {
                BigDecimal value = resultSet.getBigDecimal(index);
                yield value == null
                        ? nullValue()
                        : Value.newBuilder().setIntegerValue(value.intValueExact()).build();
            }
            case VALUE_TYPE_LONG -> {
                BigDecimal value = resultSet.getBigDecimal(index);
                yield value == null
                        ? nullValue()
                        : Value.newBuilder().setLongValue(value.longValueExact()).build();
            }
            case VALUE_TYPE_BIGINTEGER -> {
                BigDecimal value = resultSet.getBigDecimal(index);
                yield value == null
                        ? nullValue()
                        : Value.newBuilder()
                                .setBigintegerValue(value.toBigIntegerExact().toString())
                                .build();
            }
            case VALUE_TYPE_FLOAT -> {
                float value = resultSet.getFloat(index);
                yield resultSet.wasNull()
                        ? nullValue()
                        : Value.newBuilder().setFloatValue(value).build();
            }
            case VALUE_TYPE_DOUBLE -> {
                double value = resultSet.getDouble(index);
                yield resultSet.wasNull()
                        ? nullValue()
                        : Value.newBuilder().setDoubleValue(value).build();
            }
            case VALUE_TYPE_BIGDECIMAL -> {
                BigDecimal value = resultSet.getBigDecimal(index);
                yield value == null
                        ? nullValue()
                        : Value.newBuilder().setBigdecimalValue(value.toPlainString()).build();
            }
            case VALUE_TYPE_DATE -> {
                Date value = resultSet.getDate(index);
                yield value == null
                        ? nullValue()
                        : Value.newBuilder().setDateValue(value.toLocalDate().toString()).build();
            }
            case VALUE_TYPE_TIME -> {
                Time value = resultSet.getTime(index);
                yield value == null
                        ? nullValue()
                        : Value.newBuilder().setTimeValue(value.toLocalTime().toString()).build();
            }
            case VALUE_TYPE_TIMESTAMP -> {
                Timestamp value = resultSet.getTimestamp(index);
                yield value == null
                        ? nullValue()
                        : Value.newBuilder()
                                .setTimestampValue(value.toLocalDateTime().toString())
                                .build();
            }
            case VALUE_TYPE_BLOB -> readBlob(resultSet.getBlob(index));
            case VALUE_TYPE_CLOB -> readClob(resultSet.getClob(index));
            default -> throw invalid("result type is not supported");
        };
    }

    private static Value nullableString(String value, boolean character) {
        if (value == null) {
            return nullValue();
        }
        return character
                ? Value.newBuilder().setCharValue(value).build()
                : Value.newBuilder().setStringValue(value).build();
    }

    private static Value nullableBytes(byte[] value) {
        return value == null
                ? nullValue()
                : Value.newBuilder().setVarbinaryValue(ByteString.copyFrom(value)).build();
    }

    private static Value readBlob(Blob blob) throws SQLException {
        if (blob == null) {
            return nullValue();
        }
        try (InputStream stream = blob.getBinaryStream()) {
            byte[] value = stream.readNBytes(MAX_INLINE_LOB_BYTES + 1);
            if (value.length > MAX_INLINE_LOB_BYTES) {
                throw new SQLException("Oracle BLOB exceeds the inline result limit");
            }
            return Value.newBuilder()
                    .setBlobValue(BlobValue.newBuilder().setData(ByteString.copyFrom(value)))
                    .build();
        } catch (IOException exception) {
            throw new SQLException("Unable to read Oracle BLOB", exception);
        } finally {
            blob.free();
        }
    }

    private static Value readClob(Clob clob) throws SQLException {
        if (clob == null) {
            return nullValue();
        }
        try (Reader reader = clob.getCharacterStream()) {
            StringBuilder value = new StringBuilder();
            char[] buffer = new char[8192];
            int read;
            while ((read = reader.read(buffer)) >= 0) {
                if (value.length() + read > MAX_INLINE_LOB_BYTES) {
                    throw new SQLException("Oracle CLOB exceeds the inline result limit");
                }
                value.append(buffer, 0, read);
            }
            return Value.newBuilder()
                    .setClobValue(ClobValue.newBuilder().setData(value.toString()))
                    .build();
        } catch (IOException exception) {
            throw new SQLException("Unable to read Oracle CLOB", exception);
        } finally {
            clob.free();
        }
    }

    private static TypeDescriptor requestedType(Expression expression) {
        return switch (expression.getKindCase()) {
            case AGGREGATE -> expression.getAggregate().hasResultType()
                    ? expression.getAggregate().getResultType()
                    : null;
            case LITERAL -> expression.getLiteral().hasDeclaredType()
                    ? expression.getLiteral().getDeclaredType()
                    : descriptor(valueType(expression.getLiteral().getValue()));
            case COMPARISON, LOGICAL, NULL_PREDICATE, PATTERN ->
                    descriptor(ValueType.VALUE_TYPE_BOOLEAN);
            case FIELD, FUNCTION_CALL, KIND_NOT_SET -> null;
        };
    }

    private static TypeDescriptor metadataType(ResultSetMetaData metadata, int index)
            throws SQLException {
        int precision = metadata.getPrecision(index);
        int scale = metadata.getScale(index);
        TypeMapper.Mapping mapping = TypeMapper.map(
                        metadata.getColumnType(index),
                        metadata.getColumnTypeName(index),
                        precision > 0 ? precision : null,
                        scale)
                .orElseThrow(() -> invalid("Oracle result column type is not supported"));
        TypeDescriptor.Builder descriptor = TypeDescriptor.newBuilder().setType(mapping.type());
        if (mapping.precision() != null
                && (mapping.type() == ValueType.VALUE_TYPE_BIGINTEGER
                        || mapping.type() == ValueType.VALUE_TYPE_BIGDECIMAL)) {
            descriptor.setPrecision(mapping.precision());
        }
        if (mapping.scale() != null
                && mapping.precision() != null
                && mapping.type() == ValueType.VALUE_TYPE_BIGDECIMAL
                && mapping.scale() <= mapping.precision()) {
            descriptor.setScale(mapping.scale());
        }
        return descriptor.build();
    }

    private static TypeDescriptor descriptor(ValueType type) {
        return TypeDescriptor.newBuilder().setType(type).build();
    }

    private static ValueType valueType(Value value) {
        return switch (value.getKindCase()) {
            case NULL_VALUE, KIND_NOT_SET -> ValueType.VALUE_TYPE_UNKNOWN;
            case STRING_VALUE -> ValueType.VALUE_TYPE_STRING;
            case VARBINARY_VALUE -> ValueType.VALUE_TYPE_VARBINARY;
            case CHAR_VALUE -> ValueType.VALUE_TYPE_CHAR;
            case BOOLEAN_VALUE -> ValueType.VALUE_TYPE_BOOLEAN;
            case BYTE_VALUE -> ValueType.VALUE_TYPE_BYTE;
            case SHORT_VALUE -> ValueType.VALUE_TYPE_SHORT;
            case INTEGER_VALUE -> ValueType.VALUE_TYPE_INTEGER;
            case LONG_VALUE -> ValueType.VALUE_TYPE_LONG;
            case BIGINTEGER_VALUE -> ValueType.VALUE_TYPE_BIGINTEGER;
            case FLOAT_VALUE -> ValueType.VALUE_TYPE_FLOAT;
            case DOUBLE_VALUE -> ValueType.VALUE_TYPE_DOUBLE;
            case BIGDECIMAL_VALUE -> ValueType.VALUE_TYPE_BIGDECIMAL;
            case DATE_VALUE -> ValueType.VALUE_TYPE_DATE;
            case TIME_VALUE -> ValueType.VALUE_TYPE_TIME;
            case TIMESTAMP_VALUE -> ValueType.VALUE_TYPE_TIMESTAMP;
            case BLOB_VALUE -> ValueType.VALUE_TYPE_BLOB;
            case CLOB_VALUE -> ValueType.VALUE_TYPE_CLOB;
            case XML_VALUE -> ValueType.VALUE_TYPE_XML;
            case GEOMETRY_VALUE, GEOMETRY_WITH_CRS -> ValueType.VALUE_TYPE_GEOMETRY;
            case GEOGRAPHY_VALUE, GEOGRAPHY_WITH_CRS -> ValueType.VALUE_TYPE_GEOGRAPHY;
            case JSON_VALUE -> ValueType.VALUE_TYPE_JSON;
            case ARRAY_VALUE -> ValueType.VALUE_TYPE_ARRAY;
            case LOB_REFERENCE -> value.getLobReference().getType();
        };
    }

    private static int jdbcType(ValueType type) {
        requireSupported(type);
        return switch (type) {
            case VALUE_TYPE_STRING, VALUE_TYPE_CHAR -> Types.VARCHAR;
            case VALUE_TYPE_VARBINARY -> Types.VARBINARY;
            case VALUE_TYPE_BOOLEAN -> Types.BOOLEAN;
            case VALUE_TYPE_BYTE -> Types.TINYINT;
            case VALUE_TYPE_SHORT -> Types.SMALLINT;
            case VALUE_TYPE_INTEGER -> Types.INTEGER;
            case VALUE_TYPE_LONG -> Types.BIGINT;
            case VALUE_TYPE_BIGINTEGER, VALUE_TYPE_BIGDECIMAL -> Types.NUMERIC;
            case VALUE_TYPE_FLOAT -> Types.REAL;
            case VALUE_TYPE_DOUBLE -> Types.DOUBLE;
            case VALUE_TYPE_DATE -> Types.DATE;
            case VALUE_TYPE_TIME -> Types.TIME;
            case VALUE_TYPE_TIMESTAMP -> Types.TIMESTAMP;
            case VALUE_TYPE_BLOB -> Types.BLOB;
            case VALUE_TYPE_CLOB -> Types.CLOB;
            default -> throw invalid("literal type is not supported");
        };
    }

    private static void requireSupported(ValueType type) {
        if (!SUPPORTED_TYPES.contains(type)) {
            throw invalid("value type is not supported");
        }
    }

    private static Value nullValue() {
        return Value.newBuilder().setNullValue(NullValue.getDefaultInstance()).build();
    }

    private static RequestValidationException invalid(String message) {
        return new RequestValidationException(message);
    }

    private static RequestValidationException invalid(String message, Throwable cause) {
        return new RequestValidationException(message, cause);
    }

    record ResultColumn(Field field, ValueType type) {}

}
