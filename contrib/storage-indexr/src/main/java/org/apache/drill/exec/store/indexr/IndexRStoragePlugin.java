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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class IndexRStoragePlugin extends AbstractStoragePlugin {
    private static final Logger log = LoggerFactory.getLogger(IndexRStoragePlugin.class);

    private final IndexRStoragePluginConfig engineConfig;
    private final DrillbitContext context;
    private final String pluginName;

    private final IndexRSchemaFactory schemaFactory;
    private final FakeSegmentManager segmentManager;

    public IndexRStoragePlugin(IndexRStoragePluginConfig engineConfig, DrillbitContext context, String name) {
        this.engineConfig = engineConfig;
        this.context = context;
        this.pluginName = name;

        this.schemaFactory = new IndexRSchemaFactory(this);
        try {
            this.segmentManager = new FakeSegmentManager(engineConfig.getDataDir());
        } catch (IOException e) {
            log.warn("", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexRStoragePluginConfig getConfig() {
        return engineConfig;
    }

    @Override
    public boolean supportsRead() {
        return true;
    }

    @Override
    public boolean supportsWrite() {
        return false;
    }

    public DrillbitContext context() {
        return context;
    }

    public String pluginName() {
        return pluginName;
    }

    public FakeSegmentManager segmentManager() {
        return segmentManager;
    }

    @Override
    public void start() throws IOException {
        super.start();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        schemaFactory.registerSchemas(schemaConfig, parent);
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
        return getPhysicalScan(userName, selection, GroupScan.ALL_COLUMNS);
    }

    @Override
    public AbstractGroupScan getPhysicalScan(String userName, JSONOptions selection, List<SchemaPath> columns) throws IOException {
        IndexRScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<IndexRScanSpec>() {});
        return new IndexRGroupScan(this, scanSpec, columns);
    }
}
