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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@JsonTypeName("indexr-scan")
public class IndexRGroupScan extends AbstractGroupScan {
    private final IndexRStoragePlugin plugin;
    private final IndexRScanSpec scanSpec;
    private final List<SchemaPath> columns;

    private ListMultimap<Integer, String> assignments;

    @JsonCreator
    public IndexRGroupScan(@JsonProperty("indexrScanSpec") IndexRScanSpec scanSpec,
                           @JsonProperty("storage") IndexRStoragePluginConfig storagePluginConfig,
                           @JsonProperty("columns") List<SchemaPath> columns,
                           @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
        this((IndexRStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), scanSpec, columns);
    }

    public IndexRGroupScan(IndexRStoragePlugin plugin, IndexRScanSpec scanSpec, List<SchemaPath> columns) {
        super((String) null);
        this.plugin = plugin;
        this.scanSpec = scanSpec;
        this.columns = columns;
    }

    /**
     * Private constructor, used for cloning.
     */
    private IndexRGroupScan(IndexRGroupScan that) {
        super(that);
        this.columns = that.columns;
        this.scanSpec = that.scanSpec;
        this.plugin = that.plugin;
        this.assignments = that.assignments;
    }

    @JsonProperty("storage")
    public IndexRStoragePluginConfig getStorageConfig() {
        return plugin.getConfig();
    }

    @JsonProperty("columns")
    public List<SchemaPath> getColumns() {
        return columns;
    }

    @JsonProperty("indexrScanSpec")
    public IndexRScanSpec getScanSpec() {
        return scanSpec;
    }

    @Override
    public int getMaxParallelizationWidth() {
        return Math.min(Runtime.getRuntime().availableProcessors() - 1,
                plugin.segmentManager().getSegmentCount(scanSpec.getTableName()));
    }

    @Override
    public String getDigest() {
        return toString();
    }

    @Override
    public String toString() {
        return "IndexRGroupScan [IndexRScanSpec=" + scanSpec + ", columns=" + columns + "]";
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        return new IndexRGroupScan(this);
    }

    @Override
    public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
        List<Integer> localMinorFragmentIds = new ArrayList<>();
        int index = 0;
        for (DrillbitEndpoint endpoint : endpoints) {
            if (endpoint.equals(plugin.context().getEndpoint())) {
                localMinorFragmentIds.add(index);
            }
            index++;
        }
        Preconditions.checkState(localMinorFragmentIds.size() > 0);

        List<String> segmentIds = plugin.segmentManager().getSegmentIdList(scanSpec.getTableName());

        assignments = ArrayListMultimap.create();
        double fragAssignScale = (double) segmentIds.size() / localMinorFragmentIds.size();
        int count = 0;
        for (int fragId : localMinorFragmentIds) {
            for (String id : segmentIds.subList(
                    (int) (count * fragAssignScale),
                    (int) ((count + 1) * fragAssignScale))) {
                assignments.put(fragId, id);
            }
            count++;
        }

        Preconditions.checkState(assignments.values().size() == segmentIds.size());
    }

    @Override
    public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
        IndexRSubScanSpec subScanSpec = new IndexRSubScanSpec(scanSpec.getTableName(), assignments.get(minorFragmentId));
        return new IndexRSubScan(plugin, subScanSpec, columns);
    }
}
