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
import io.indexr.segment.Segment;
import io.indexr.segment.helper.SegmentAssigner;
import jodd.cache.LRUCache;
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
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IndexRScanBatchCreator implements BatchCreator<IndexRSubScan> {
  static final Logger logger = LoggerFactory.getLogger(IndexRScanBatchCreator.class);

  private LRUCache<String, Map<Integer, List<SegmentAssigner.Assignment>>> assigmentCache = new LRUCache<String, Map<Integer, List<SegmentAssigner.Assignment>>>(64, TimeUnit.MINUTES.toMillis(1));

  @Override
  public ScanBatch getBatch(FragmentContext context, IndexRSubScan subScan, List<RecordBatch> children) throws ExecutionSetupException {
    logger.debug("=====================  getBatch subScan.getSpec - " + subScan.getSpec());
    Preconditions.checkArgument(children.isEmpty());

    FakeSegmentManager segmentManager = subScan.getPlugin().segmentManager();
    IndexRSubScanSpec spec = subScan.getSpec();
    // TODO This should refactor for production used!
    Map<Integer, List<SegmentAssigner.Assignment>> assigmentMap = assigmentCache.get(spec.scanId);
    if (assigmentMap == null) {
      List<Segment> segments = segmentManager.getSegmentList(spec.tableName);
      List<Segment> solidified = new ArrayList<>(segments.size());
      for (Segment segment : segments) {
        solidified.add(segment.solidify());
      }
      assigmentMap = SegmentAssigner.assignBalance(spec.parallelization, solidified, spec.rsFilter);
      assigmentCache.put(spec.scanId, assigmentMap);
    }

    List<SegmentAssigner.Assignment> assignmentList = assigmentMap.get(spec.parallelizationIndex);

    List<RecordReader> assignReaders = new ArrayList<>();
    if (assignmentList == null || assignmentList.isEmpty()) {
      logger.warn("==========subScan with spec {} have not record reader to assign", spec);
      assignReaders.add(new EmptyRecordReader(segmentManager.getSchema(spec.tableName)));
    } else {
      logger.debug("==========assignmentList: {}", assignmentList);
      for (SegmentAssigner.Assignment assignment : assignmentList) {
        assignReaders.add(IndexRRecordReader.create(//
          spec.tableName, //
          assignment.segment, //
          subScan.getColumns(), //
          context, //
          spec.rsFilter,//
          assignment.fromPack,//
          assignment.packCount));
      }
    }
    return new ScanBatch(subScan, context, assignReaders.iterator());
  }
}
