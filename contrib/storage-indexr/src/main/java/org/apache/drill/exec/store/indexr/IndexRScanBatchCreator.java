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
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import io.indexr.segment.Segment;

public class IndexRScanBatchCreator implements BatchCreator<IndexRSubScan> {
    static final Logger logger = LoggerFactory.getLogger(IndexRScanBatchCreator.class);

    @Override
    public ScanBatch getBatch(FragmentContext context, IndexRSubScan subScan, List<RecordBatch> children)
            throws ExecutionSetupException {
        logger.debug("=====================  getBatch subScan.getSpec - " + subScan.getSpec());

        Preconditions.checkArgument(children.isEmpty());

        FakeSegmentManager segmentManager = subScan.getPlugin().segmentManager();
        IndexRSubScanSpec spec = subScan.getSpec();
        List<Segment> segments = segmentManager.getSegmentList(spec.tableName);

        List<Segment> toScanSegments = new ArrayList<>();
        if (spec.parallelization >= segments.size()) {
            if (spec.parallelizationIndex >= segments.size()) {
                logger.warn("subScan with spec {} have not record reader to assign", spec);
            } else {
                toScanSegments.add(segments.get(spec.parallelizationIndex));
            }
        } else {
            double assignScale = ((double) segments.size() / spec.parallelization);
            toScanSegments = segments.subList(
                    (int) (spec.parallelizationIndex * assignScale),
                    (int) ((spec.parallelizationIndex + 1) * assignScale)
            );
        }

        logger.debug("==========toScanSegments: {}", toScanSegments);

        List<RecordReader> assignReaders = new ArrayList<>();
        if (toScanSegments.isEmpty()) {
            // Make drill happy.
            assignReaders.add(new EmptyRecordReader(segmentManager.getSchema(spec.tableName)));
        } else {
            for (Segment segment : toScanSegments) {
                assignReaders.add(IndexRRecordReader.create(
                        spec.tableName,
                        segment.solidify(),
                        subScan.getColumns(),
                        context,
                        spec.rsFilter));
            }
        }
        return new ScanBatch(subScan, context, assignReaders.iterator());
    }
}
