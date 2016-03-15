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
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;

import java.util.ArrayList;
import java.util.List;

import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.SegmentSchema;

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
            case ColumnType.Int:
            case ColumnType.Long:
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            case ColumnType.Float:
                return typeFactory.createSqlType(SqlTypeName.FLOAT);
            case ColumnType.Double:
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            case ColumnType.String:
                return typeFactory.createSqlType(SqlTypeName.VARCHAR, ColumnType.MAX_STRING_SIZE);
            default:
                throw new UnsupportedOperationException(String.format("Unsupported type [%s]", type));
        }
    }

    public static MinorType parseMinorType(byte type) {
        switch (type) {
            case ColumnType.Int:
                return MinorType.INT;
            case ColumnType.Long:
                return MinorType.BIGINT;
            case ColumnType.Float:
                return MinorType.FLOAT4;
            case ColumnType.Double:
                return MinorType.FLOAT8;
            case ColumnType.String:
                return MinorType.VARCHAR;
            default:
                throw new UnsupportedOperationException(String.format("Unsupported type [%s]", type));
        }
    }
}
