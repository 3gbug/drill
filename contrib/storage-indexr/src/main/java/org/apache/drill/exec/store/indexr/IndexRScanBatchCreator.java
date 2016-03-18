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

import com.google.common.base.Preconditions;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.indexr.segment.Segment;

public class IndexRScanBatchCreator implements BatchCreator<IndexRSubScan> {
    static final Logger logger = LoggerFactory.getLogger(IndexRScanBatchCreator.class);

    @Override
    public ScanBatch getBatch(FragmentContext context, IndexRSubScan subScan, List<RecordBatch> children)
            throws ExecutionSetupException {
        logger.debug("=====================  getBatch subScan - " + subScan);

        Preconditions.checkArgument(children.isEmpty());

        FakeSegmentManager segmentManager = subScan.getPlugin().segmentManager();
        Map<String, Segment> idToSegment = segmentManager.getSegmentMap(subScan.getSpec().tableName);
        List<Segment> segments = segmentManager.getSegmentList(subScan.getSpec().tableName);
        IndexRSubScanSpec spec = subScan.getSpec();

        List<RecordReader> assignReaders;
        if (spec.parallelization > idToSegment.size()) {
            if (spec.parallelizationIndex >= idToSegment.size()) {
                logger.warn("subScan with spec {} have not record reader to assign", spec);
                assignReaders = Collections.<RecordReader>singletonList(new EmptyRecordReader());
            } else {
                assignReaders = Collections.<RecordReader>singletonList(
                        new IndexRRecordReader(segments.get(spec.parallelizationIndex), subScan.getColumns(), context));
            }
        } else {
            double assignScale = ((double) spec.parallelization) / idToSegment.size();
            List<Segment> assignSegments = segments.subList(
                    (int) (spec.parallelizationIndex * assignScale),
                    (int) ((spec.parallelizationIndex + 1) * assignScale)
            );
            assignReaders = assignSegments.stream().map(
                    s -> (RecordReader) new IndexRRecordReader(s, subScan.getColumns(), context))
                    .collect(Collectors.toList());
        }
        return new ScanBatch(subScan, context, assignReaders.iterator());
    }

    static class EmptyRecordReader extends AbstractRecordReader {
        @Override public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {}

        @Override public int next() {return 0;}

        @Override public void close() throws Exception {}
    }
}
