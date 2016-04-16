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

import io.indexr.segment.SegmentSchema;
import io.indexr.segment.rc.RCOperator;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexRPushDownRSFilter {
  private static final Logger log = LoggerFactory.getLogger(IndexRPushDownRSFilter.class);

  public static StoragePluginOptimizerRule Scan = new StoragePluginOptimizerRule(RelOptHelper.any(ScanPrel.class), "IndexRFilterScan") {
    @Override
    public void onMatch(RelOptRuleCall call) {
      ScanPrel scan = (ScanPrel) call.rel(0);
      GroupScan gs = scan.getGroupScan();
      if (gs == null || !(gs instanceof IndexRGroupScan)) {
        return;
      }
      IndexRGroupScan groupScan = (IndexRGroupScan) gs;
      if (groupScan.getScanSpec().getRSFilter() != null) {
        return;
      }
      SegmentSchema schema = groupScan.getStoragePlugin().segmentManager().getSchema(groupScan.getScanSpec().getTableName());
      RCOperator rsFilter = new RSFilterGenerator(schema).gen(scan.getFilter());
      log.debug("================= rsFilter:" + rsFilter);
      groupScan.getScanSpec().setRSFilter(rsFilter);
    }
  };
}
