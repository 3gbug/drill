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

import java.util.Iterator;
import java.util.List;

import io.indexr.segment.ColumnType;
import io.indexr.segment.Row;
import io.indexr.segment.Segment;
import io.indexr.segment.pack.DataPack;
import io.indexr.segment.rc.RCOperator;

public class IndexRRecordReaderByRow extends IndexRRecordReader {
    private static final int TARGET_RECORD_COUNT = DataPack.MAX_COUNT >>> 4; // 4096

    private Iterator<Row> segmentRowItr;

    public IndexRRecordReaderByRow(String tableName,
                                   Segment segment,
                                   List<SchemaPath> projectColumns,
                                   FragmentContext context,
                                   RCOperator rsFilter) {
        super(tableName, segment, projectColumns, context, rsFilter);
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
        super.setup(context, output);
        segmentRowItr = segment.rowTraversal().iterator();
    }

    @Override
    public int next() {
        // By Row.
        int rowCount = 0;
        while (rowCount < TARGET_RECORD_COUNT && segmentRowItr.hasNext()) {
            Row row = segmentRowItr.next();
            for (ProjectedColumnInfo info : projectedColumnInfos) {
                switch (info.dataType) {
                    case ColumnType.INT:
                        ((IntVector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, row.getInt(info.columnId));
                        break;
                    case ColumnType.LONG:
                        ((BigIntVector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, row.getLong(info.columnId));
                        break;
                    case ColumnType.FLOAT:
                        ((Float4Vector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, row.getFloat(info.columnId));
                        break;
                    case ColumnType.DOUBLE:
                        ((Float8Vector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, row.getDouble(info.columnId));
                        break;
                    case ColumnType.STRING:
                        CharSequence value = row.getString(info.columnId);
                        ((VarCharVector.Mutator) info.valueVector.getMutator()).setSafe(rowCount, value.toString().getBytes());
                        break;
                    default:
                        throw new IllegalStateException(String.format("Unhandled date type %s", info.dataType));
                }
            }
            rowCount++;
        }
        return rowCount;
    }
}
