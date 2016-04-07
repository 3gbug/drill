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
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.ValueVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import io.indexr.segment.Column;
import io.indexr.segment.ColumnSchema;
import io.indexr.segment.Segment;
import io.indexr.segment.rc.RCOperator;
import io.indexr.util.Pair;

public abstract class IndexRRecordReader extends AbstractRecordReader {
    private static final Logger log = LoggerFactory.getLogger(IndexRRecordReader.class);

    final Segment segment;
    final RCOperator rsFilter;
    ProjectedColumnInfo[] projectedColumnInfos;

    static class ProjectedColumnInfo {
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

    IndexRRecordReader(Segment segment, List<SchemaPath> projectColumns, FragmentContext context, RCOperator rsFilter) {
        this.segment = segment;
        this.rsFilter = rsFilter;

        log.debug("segment: {}", segment);

        setColumns(projectColumns);
    }

    public static IndexRRecordReader create(Segment segment, List<SchemaPath> projectColumns, FragmentContext context, RCOperator rsFilter) {
        return segment.column(0) == null
                ? new IndexRRecordReaderByRow(segment, projectColumns, context, rsFilter)
                : new IndexRRecordReaderByColumn(segment, projectColumns, context, rsFilter);
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
                Pair<ColumnSchema, Integer> p = DrillIndexRTable.mapColumn(segment.schema(), schemaPath);
                if (p == null) {
                    throw new RuntimeException(String.format(
                            "Column not found! SchemaPath: %s, search segment schema: %s",
                            schemaPath,
                            schemas
                    ));
                }
                projectedColumnInfos[count] = genPCI(p.second, p.first.dataType, p.first.name, output);
                count++;
            }
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
