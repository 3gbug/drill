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

import org.apache.commons.lang.StringUtils;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

import io.indexr.data.BytePiece;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.ColumnType;
import io.indexr.segment.Row;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.DataPack;
import io.indexr.util.MemoryUtil;

public class IndexRRecordReader extends AbstractRecordReader {
    private static final Logger log = LoggerFactory.getLogger(IndexRRecordReader.class);
    private static final int TARGET_RECORD_COUNT = DataPack.MAX_COUNT >>> 4; // 4096


    private final Segment segment;

    private ProjectedColumnInfo[] projectedColumnInfos;
    private Iterator<Row> segmentRowItr;
    private long curRowIndex = 0;

    private static class ProjectedColumnInfo {
        int ordinal;
        byte dataType;
        ValueVector valueVector;

        Column column;

        public ProjectedColumnInfo(int ordinal, byte dataType, ValueVector valueVector, Column column) {
            this.ordinal = ordinal;
            this.dataType = dataType;
            this.valueVector = valueVector;
            this.column = column;
        }
    }

    public IndexRRecordReader(Segment segment, List<SchemaPath> projectColumns, FragmentContext context) {
        this.segment = segment;

        log.info("=====================  IndexRRecordReader projectColumns - " + projectColumns);

        log.debug("segment: {}", segment);

        setColumns(projectColumns);
    }

    @SuppressWarnings("unchecked")
    private ProjectedColumnInfo genPCI(int ordinal, byte dataType, String name, OutputMutator output) {
        TypeProtos.MinorType minorType = DrillIndexRTable.parseMinorType(dataType);
        TypeProtos.MajorType majorType = Types.required(minorType);
        MaterializedField field = MaterializedField.create(name, majorType);
        final Class<? extends ValueVector> clazz = (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(
                minorType, majorType.getMode());
        ValueVector vector = null;
        try {
            vector = output.addField(field, clazz);
        } catch (SchemaChangeException e) {
            throw new RuntimeException(e);
        }
        vector.allocateNew();
        Column column = segment.column(ordinal);
        return new ProjectedColumnInfo(ordinal, dataType, vector, column);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
        List<ColumnSchema> schemas = segment.schema().columns;
        if (isStarQuery()) {
            projectedColumnInfos = new ProjectedColumnInfo[schemas.size()];
            int ordinal = 0;
            for (ColumnSchema cs : schemas) {
                projectedColumnInfos[ordinal] = genPCI(ordinal, cs.dataType, cs.name, output);
                ordinal++;
            }
        } else {
            projectedColumnInfos = new ProjectedColumnInfo[this.getColumns().size()];
            int count = 0;
            for (SchemaPath schemaPath : this.getColumns()) {
                String colName = StringUtils.removeStart(
                        schemaPath.getAsUnescapedPath().toLowerCase(),
                        segment.schema().name + "."); // remove the table name.
                int[] ordinal = new int[]{-1};
                ColumnSchema cs = schemas.stream().filter(
                        s -> {
                            ordinal[0]++;
                            return s.name.equalsIgnoreCase(colName);
                        })
                        .findFirst().get();
                if (cs == null) {
                    throw new RuntimeException(String.format(
                            "Column not found! SchemaPath: %s, search colName: %s, search segment schema: %s",
                            schemaPath,
                            colName,
                            schemas
                    ));
                }
                projectedColumnInfos[count] = genPCI(ordinal[0], cs.dataType, cs.name, output);
                count++;
            }
        }

        // If we cannot fetch data by column, then do it by row.
        if (projectedColumnInfos.length > 0) {
            if (projectedColumnInfos[0].column == null) {
                segmentRowItr = segment.rowTraversal().iterator();
            }
        }
    }


    @Override
    public int next() {
        if (segmentRowItr != null) {
            // By Row.
            int rowCount = 0;
            while (rowCount < TARGET_RECORD_COUNT && segmentRowItr.hasNext()) {
                Row row = segmentRowItr.next();
                for (ProjectedColumnInfo info : projectedColumnInfos) {
                    switch (info.dataType) {
                        case ColumnType.Int:
                            ((IntVector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, row.getInt(info.ordinal));
                            break;
                        case ColumnType.Long:
                            ((BigIntVector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, row.getLong(info.ordinal));
                            break;
                        case ColumnType.Float:
                            ((Float4Vector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, row.getFloat(info.ordinal));
                            break;
                        case ColumnType.Double:
                            ((Float8Vector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, row.getDouble(info.ordinal));
                            break;
                        case ColumnType.String:
                            CharSequence value = row.getString(info.ordinal);
                            ((VarCharVector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, value.toString().getBytes());
                            break;
                        default:
                            throw new RuntimeException("Impossible!");
                    }
                }
                rowCount++;
            }
            return rowCount;
        } else {
            // By column;
            // TODO add index & agg optimize.
            BytePiece bytePiece = new BytePiece();
            ByteBuffer byteBuffer = MemoryUtil.getHollowDirectByteBuffer();
            int rowCount = 0;
            while (rowCount < TARGET_RECORD_COUNT && curRowIndex < segment.rowCount()) {
                for (ProjectedColumnInfo info : projectedColumnInfos) {
                    switch (info.dataType) {
                        case ColumnType.Int:
                            ((IntVector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, info.column.intValueAt(curRowIndex));
                            break;
                        case ColumnType.Long:
                            ((BigIntVector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, info.column.longValueAt(curRowIndex));
                            break;
                        case ColumnType.Float:
                            ((Float4Vector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, info.column.floatValueAt(curRowIndex));
                            break;
                        case ColumnType.Double:
                            ((Float8Vector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, info.column.doubleValueAt(curRowIndex));
                            break;
                        case ColumnType.String:
                            info.column.rawValueAt(curRowIndex, bytePiece);
                            MemoryUtil.setByteBuffer(byteBuffer, bytePiece.addr, bytePiece.len, null);
                            ((VarCharVector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, byteBuffer, 0, byteBuffer.remaining());
                            break;
                        default:
                            throw new RuntimeException("Impossible!");
                    }
                }
                rowCount++;
                curRowIndex++;
            }
            return rowCount;
        }
    }

    @Override
    public void close() throws Exception {
        for (ProjectedColumnInfo info : projectedColumnInfos) {
            if (info.column != null) {
                info.column.free();
            }
        }
    }
}
