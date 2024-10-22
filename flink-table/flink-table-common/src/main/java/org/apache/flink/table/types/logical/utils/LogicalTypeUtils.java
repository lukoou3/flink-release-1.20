/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Utilities for handling {@link LogicalType}s. */
@Internal
public final class LogicalTypeUtils {

    private static final String ATOMIC_FIELD_NAME = "f0";

    private static final TimeAttributeRemover TIME_ATTRIBUTE_REMOVER = new TimeAttributeRemover();

    public static LogicalType removeTimeAttributes(LogicalType logicalType) {
        return logicalType.accept(TIME_ATTRIBUTE_REMOVER);
    }

    /**
     * 看看对应的内部内部类型，内部使用什么类型储存，RowData类的注释也有转换表格。
     * Returns the conversion class for the given {@link LogicalType} that is used by the table
     * runtime as internal data structure.
     *
     * @see RowData
     */
    public static Class<?> toInternalConversionClass(LogicalType type) {
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                // CharType(length) => CHAR
                // VarCharType(length) => VARCHAR
                // string(max length varChar) => VARCHAR
                // CHAR / VARCHAR / STRING: StringData
                return StringData.class;
            case BOOLEAN:
                // BooleanType => BOOLEAN
                // BOOLEAN: boolean
                return Boolean.class;
            case BINARY:
            case VARBINARY:
                // BinaryType => BINARY
                // VarBinaryType => VARBINARY
                // BINARY / VARBINARY / BYTES: byte[]
                return byte[].class;
            case DECIMAL:
                // DecimalType => DECIMAL
                // DECIMAL: DecimalData
                return DecimalData.class;
            case TINYINT:
                // TinyIntType => TINYINT
                // TINYINT: byte
                return Byte.class;
            case SMALLINT:
                // SmallIntType => SMALLINT
                // SMALLINT: short
                return Short.class;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                // IntType => INTEGER
                // DateType => DATE
                // TimeType => TIME_WITHOUT_TIME_ZONE
                // YearMonthIntervalType => INTERVAL_YEAR_MONTH
                // INT/DATE/TIME/INTERVAL YEAR TO MONTH: int
                return Integer.class;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                // BigIntType => BIGINT
                // DayTimeIntervalType => INTERVAL_DAY_TIME
                // BIGINT/INTERVAL DAY TO MONTH: long
                return Long.class;
            case FLOAT:
                return Float.class;
            case DOUBLE:
                return Double.class;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // TimestampType => TIMESTAMP_WITHOUT_TIME_ZONE
                // LocalZonedTimestampType => TIMESTAMP_WITH_LOCAL_TIME_ZONE
                // TIMESTAMP/TIMESTAMP WITH LOCAL TIME ZONE: TimestampData
                return TimestampData.class;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException("Unsupported type: " + type);
            case ARRAY:
                return ArrayData.class;
            case MULTISET:
            case MAP:
                return MapData.class;
            case ROW:
            case STRUCTURED_TYPE:
                // ROW / structured types: RowData
                return RowData.class;
            case DISTINCT_TYPE:
                return toInternalConversionClass(((DistinctType) type).getSourceType());
            case RAW:
                return RawValueData.class;
            case NULL:
                return Object.class;
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException("Illegal type: " + type);
        }
    }

    /**
     * Converts any logical type to a row type. Composite types are converted to a row type. Atomic
     * types are wrapped into a field.
     */
    public static RowType toRowType(LogicalType t) {
        switch (t.getTypeRoot()) {
            case ROW:
                return (RowType) t;
            case STRUCTURED_TYPE:
                final StructuredType structuredType = (StructuredType) t;
                final List<RowField> fields =
                        structuredType.getAttributes().stream()
                                .map(
                                        attribute ->
                                                new RowField(
                                                        attribute.getName(),
                                                        attribute.getType(),
                                                        attribute.getDescription().orElse(null)))
                                .collect(Collectors.toList());
                return new RowType(structuredType.isNullable(), fields);
            case DISTINCT_TYPE:
                return toRowType(((DistinctType) t).getSourceType());
            default:
                return RowType.of(t);
        }
    }

    /** Returns a unique name for an atomic type. */
    public static String getAtomicName(List<String> existingNames) {
        int i = 0;
        String fieldName = ATOMIC_FIELD_NAME;
        while ((null != existingNames) && existingNames.contains(fieldName)) {
            fieldName = ATOMIC_FIELD_NAME + "_" + i++;
        }
        return fieldName;
    }

    /** Renames the fields of the given {@link RowType}. */
    public static RowType renameRowFields(RowType rowType, List<String> newFieldNames) {
        Preconditions.checkArgument(
                rowType.getFieldCount() == newFieldNames.size(),
                "Row length and new names must match.");
        final List<RowField> newFields =
                IntStream.range(0, rowType.getFieldCount())
                        .mapToObj(
                                pos -> {
                                    final RowField oldField = rowType.getFields().get(pos);
                                    return new RowField(
                                            newFieldNames.get(pos),
                                            oldField.getType(),
                                            oldField.getDescription().orElse(null));
                                })
                        .collect(Collectors.toList());
        return new RowType(rowType.isNullable(), newFields);
    }

    // --------------------------------------------------------------------------------------------

    private static class TimeAttributeRemover extends LogicalTypeDuplicator {

        @Override
        public LogicalType visit(TimestampType timestampType) {
            return new TimestampType(timestampType.isNullable(), timestampType.getPrecision());
        }

        @Override
        public LogicalType visit(ZonedTimestampType zonedTimestampType) {
            return new ZonedTimestampType(
                    zonedTimestampType.isNullable(), zonedTimestampType.getPrecision());
        }

        @Override
        public LogicalType visit(LocalZonedTimestampType localZonedTimestampType) {
            return new LocalZonedTimestampType(
                    localZonedTimestampType.isNullable(), localZonedTimestampType.getPrecision());
        }
    }

    private LogicalTypeUtils() {
        // no instantiation
    }
}
