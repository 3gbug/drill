/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;

import java.util.ArrayList;
import java.util.List;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.SegmentSchema;
import io.indexr.util.Pair;

public class DrillIndexRTable extends DynamicDrillTable {
    private final SegmentSchema schema;

    public DrillIndexRTable(IndexRStoragePlugin plugin, IndexRScanSpec spec, SegmentSchema schema) {
        super(plugin, plugin.pluginName(), spec);
        this.schema = schema;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();
        for (ColumnSchema cs : schema.columns) {
            names.add(cs.name);
            types.add(parseDataType(typeFactory, cs.dataType));
        }
        return typeFactory.createStructType(types, names);
    }

    public static RelDataType parseDataType(RelDataTypeFactory typeFactory, byte type) {
        switch (type) {
            case ColumnType.INT:
            case ColumnType.LONG:
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            case ColumnType.FLOAT:
                return typeFactory.createSqlType(SqlTypeName.FLOAT);
            case ColumnType.DOUBLE:
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            case ColumnType.STRING:
                return typeFactory.createSqlType(SqlTypeName.VARCHAR, ColumnType.MAX_STRING_SIZE);
            default:
                throw new UnsupportedOperationException(String.format("Unsupported type [%s]", type));
        }
    }

    public static MinorType parseMinorType(byte type) {
        switch (type) {
            case ColumnType.INT:
                return MinorType.INT;
            case ColumnType.LONG:
                return MinorType.BIGINT;
            case ColumnType.FLOAT:
                return MinorType.FLOAT4;
            case ColumnType.DOUBLE:
                return MinorType.FLOAT8;
            case ColumnType.STRING:
                return MinorType.VARCHAR;
            default:
                throw new UnsupportedOperationException(String.format("Unsupported type [%s]", type));
        }
    }

    public static Pair<ColumnSchema, Integer> mapColumn(String tableName, SegmentSchema segmentSchema, SchemaPath schemaPath) {
        String colName = toColName(tableName, schemaPath);
        return mapColumn(segmentSchema, colName);
    }

    public static String toColName(String tableName, SchemaPath schemaPath) {
        return StringUtils.removeStart(
                schemaPath.getAsUnescapedPath().toLowerCase(),
                tableName + "."); // remove the table name.
    }

    public static Pair<ColumnSchema, Integer> mapColumn(SegmentSchema segmentSchema, String colName) {
        int[] ordinal = new int[]{-1};
        ColumnSchema cs = segmentSchema.columns.stream().filter(
                s -> {
                    ordinal[0]++;
                    return s.name.equalsIgnoreCase(colName);
                })
                .findFirst().get();
        return cs == null ? null : Pair.of(cs, ordinal[0]);
    }
}
