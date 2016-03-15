/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import com.google.common.base.Preconditions;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.indexr.segment.Segment;

public class IndexRScanBatchCreator implements BatchCreator<IndexRSubScan> {
    static final Logger logger = LoggerFactory.getLogger(IndexRScanBatchCreator.class);

    @Override
    public ScanBatch getBatch(FragmentContext context, IndexRSubScan subScan, List<RecordBatch> children)
            throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());

        FakeSegmentManager segmentManager = subScan.getPlugin().segmentManager();
        Map<String, Segment> idToSegment = segmentManager.getSegmentMap(subScan.getSpec().tableName);
        IndexRSubScanSpec spec = subScan.getSpec();

        List<RecordReader> readers = new ArrayList<>();
        for (String segmentId : spec.segmentIds) {
            Segment segment = idToSegment.get(segmentId);
            if (segment == null) {
                throw new RuntimeException(String.format("%s segment with id[%s] not found", spec.tableName, segmentId));
            }
            List<SchemaPath> columns = subScan.getColumns();
            readers.add(new IndexRRecordReader(segment, columns, context));
        }
        return new ScanBatch(subScan, context, readers.iterator());
    }
}
