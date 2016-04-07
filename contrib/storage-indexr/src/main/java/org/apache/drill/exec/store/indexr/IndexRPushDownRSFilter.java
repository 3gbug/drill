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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.indexr.segment.rc.RCOperator;

public class IndexRPushDownRSFilter {
    private static final Logger log = LoggerFactory.getLogger(IndexRPushDownRSFilter.class);

    private static void setRSFilter(RelOptRuleCall call, ScanPrel scan, FilterPrel filter) {
        final RexNode condition = filter.getCondition();
        GroupScan gs = scan.getGroupScan();
        if (gs == null || !(gs instanceof IndexRGroupScan)) {
            return;
        }

        IndexRGroupScan groupScan = (IndexRGroupScan) gs;
        if (groupScan.hasSetRSFilter()) {
            return;
        }

        LogicalExpression conditionExp = DrillOptiq.toDrill(
                new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
        RSFilterGenerator generator = new RSFilterGenerator(groupScan, conditionExp);
        RCOperator rsFilter = generator.rsFilter();
        log.info("================= rsFilter:" + rsFilter);
        groupScan.getScanSpec().setRsFilter(rsFilter);
    }

    public static StoragePluginOptimizerRule MatchFilterScan = new StoragePluginOptimizerRule(
            RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)), "IndexRFilterScan") {
        @Override
        public void onMatch(RelOptRuleCall call) {
            setRSFilter(call, (ScanPrel) call.rel(1), (FilterPrel) call.rel(0));
        }
    };

    public static StoragePluginOptimizerRule MatchFilterProjectScan = new StoragePluginOptimizerRule(
            RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))), "IndexRFilterProjectScan") {
        @Override
        public void onMatch(RelOptRuleCall call) {
            setRSFilter(call, (ScanPrel) call.rel(2), (FilterPrel) call.rel(0));
        }
    };
    //
    //public static StoragePluginOptimizerRule MatchHashJoinValuesScan = new StoragePluginOptimizerRule(
    //        //RelOptHelper.some(HashJoinPrel.class, RelOptHelper.any(ScanPrel.class)), "IndexRHashJoinValuesScan") {
    //        RelOptHelper.any(HashJoinPrel.class), "IndexRHashJoinValuesScan") {
    //
    //    private ValuesPrel valuesPrel;
    //    private ScanPrel scanPrel;
    //
    //    @Override
    //    public void onMatch(RelOptRuleCall call) {
    //        HashJoinPrel hashJoin = (HashJoinPrel) call.rel(0);
    //
    //        ValuesPrel valuesPrel;
    //        ScanPrel scanPrel;
    //
    //        RelNode left = checkNode(hashJoin, Lists.newArrayList(BroadcastExchangePrel.class, HashAggPrel.class, ValuesPrel.class));
    //        RelNode right = checkNode(hashJoin, Lists.newArrayList(SelectionVectorRemoverPrel.class, FilterPrel.class, ProjectPrel.class,  ScanPrel.class));
    //        if(left != null && right != null){
    //            valuesPrel = (ValuesPrel) left;
    //            scanPrel = (ScanPrel) right;
    //        }else {
    //            left = checkNode(hashJoin, Lists.newArrayList(SelectionVectorRemoverPrel.class, FilterPrel.class, ProjectPrel.class,  ScanPrel.class));
    //            right = checkNode(hashJoin, Lists.newArrayList(BroadcastExchangePrel.class, HashAggPrel.class, ValuesPrel.class));
    //            if(left != null && right != null){
    //                valuesPrel = (ValuesPrel) right;
    //                scanPrel = (ScanPrel) left;
    //            }else {
    //                return;
    //            }
    //        }
    //
    //    }
    //
    //    @Override
    //    public boolean matches(RelOptRuleCall call) {
    //        HashJoinPrel hashJoin = (HashJoinPrel) call.rel(0);
    //
    //        ValuesPrel valuesPrel;
    //        ScanPrel scanPrel;
    //
    //        RelNode left = checkNode(hashJoin, Lists.newArrayList(BroadcastExchangePrel.class, HashAggPrel.class, ValuesPrel.class))
    //        RelNode right = checkNode(hashJoin, Lists.newArrayList(SelectionVectorRemoverPrel.class, FilterPrel.class, ProjectPrel.class,  ScanPrel.class));
    //        if(left == null || right == null){
    //            return false;
    //        }
    //
    //        boolean[] ok = new boolean[2];
    //        hashJoin.forEach(prel -> {
    //
    //            if (prel instanceof ScanPrel) {
    //                ok[1] = true;
    //            }
    //        });
    //        return ok[0] & ok[1];
    //    }
    //
    //    private RelNode checkNode(RelNode node, List<Class<? extends RelNode>> clazzes) {
    //        if(clazzes.get(0).isInstance(node)){
    //            return null;
    //        }
    //        if(clazzes.size() == 1){
    //            return node;
    //        }
    //        List<RelNode> children = node.getInputs();
    //        if (children.isEmpty()) {
    //            return null;
    //        }
    //        return checkNode(children.get(0), clazzes.subList(1, clazzes.size()));
    //    }
    //};


}
