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

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

import io.indexr.data.BytePiece;
import io.indexr.segment.Column;
import io.indexr.segment.ColumnType;
import io.indexr.segment.RSValue;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.rc.RCOperator;
import io.indexr.util.MemoryUtil;

public class IndexRRecordReaderByColumn extends IndexRRecordReader {
    private static final Logger log = LoggerFactory.getLogger(IndexRRecordReaderByColumn.class);
    private static final int TARGET_RECORD_COUNT = DataPack.MAX_COUNT;

    private BytePiece bytePiece = new BytePiece();
    private ByteBuffer byteBuffer = MemoryUtil.getHollowDirectByteBuffer();

    private Column[] columns;
    private byte[] packRSResults;
    private int curPackId = 0;

    public IndexRRecordReaderByColumn(Segment segment, List<SchemaPath> projectColumns, FragmentContext context, RCOperator rsFilter) {
        super(segment, projectColumns, context, rsFilter);
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
        super.setup(context, output);
        columns = new Column[segment.schema().columns.size()];
        for (int i = 0; i < columns.length; i++) {
            columns[i] = segment.column(i);
        }
        if (rsFilter == null) {
            packRSResults = new byte[columns[0].packCount()];
            for (int i = 0; i < packRSResults.length; i++) {
                packRSResults[i] = RSValue.Some;
            }
        } else {
            packRSResults = rsFilter.roughCheck(columns);
        }
    }

    @Override
    public int next() {
        log.error("=========next(), segment: {}, rsFilter:{}, curPackId: {}", segment, rsFilter, curPackId);
        int rowCount = 0;
        while (curPackId < packRSResults.length && rowCount < TARGET_RECORD_COUNT) {
            byte rsValue = packRSResults[curPackId];
            switch (rsValue) {
                case RSValue.None:
                    log.error("=========rs filter ignore pack {}", curPackId);
                    // Ignore pack.
                    break;
                case RSValue.Some:
                    rowCount += processSomePack(curPackId, rowCount);
                    break;
                case RSValue.All:
                    log.error("=========rs filter found ALL pack {}", curPackId);
                    rowCount += processSomePack(curPackId, rowCount);
                    break;
                default:
                    throw new IllegalStateException("error rs value: " + rsValue);
            }
            curPackId++;
        }
        return rowCount;
    }

    private int processSomePack(int packId, int preRowCount) {
        // Only filter none pack for now.
        // TODO handle All case.
        DataPack[] dataPacks = new DataPack[projectedColumnInfos.length];
        for (int i = 0; i < projectedColumnInfos.length; i++) {
            dataPacks[i] = columns[projectedColumnInfos[i].columnId].pack(packId);
        }
        long rowCount = columns[0].dpn(packId).objCount();
        int rowIdInPack = 0;
        while (rowIdInPack < rowCount) {
            for (int i = 0; i < projectedColumnInfos.length; i++) {
                ProjectedColumnInfo info = projectedColumnInfos[i];
                switch (info.dataType) {
                    case ColumnType.INT:
                        ((IntVector.Mutator) info.valueVector.getMutator()).setSafe(preRowCount + rowIdInPack, dataPacks[i].intValueAt(rowIdInPack));
                        break;
                    case ColumnType.LONG:
                        ((BigIntVector.Mutator) info.valueVector.getMutator()).setSafe(preRowCount + rowIdInPack, dataPacks[i].longValueAt(rowIdInPack));
                        break;
                    case ColumnType.FLOAT:
                        ((Float4Vector.Mutator) info.valueVector.getMutator()).setSafe(preRowCount + rowIdInPack, dataPacks[i].floatValueAt(rowIdInPack));
                        break;
                    case ColumnType.DOUBLE:
                        ((Float8Vector.Mutator) info.valueVector.getMutator()).setSafe(preRowCount + rowIdInPack, dataPacks[i].doubleValueAt(rowIdInPack));
                        break;
                    case ColumnType.STRING:
                        dataPacks[i].rawValueAt(rowIdInPack, bytePiece);
                        MemoryUtil.setByteBuffer(byteBuffer, bytePiece.addr, bytePiece.len, null);
                        ((VarCharVector.Mutator) info.valueVector.getMutator()).setSafe(preRowCount + rowIdInPack, byteBuffer, 0, byteBuffer.remaining());
                        break;
                    default:
                        throw new IllegalStateException(String.format("Unhandled date type %s", info.dataType));
                }
            }
            rowIdInPack++;
        }
        return rowIdInPack;
    }

}
