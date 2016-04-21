/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.indexr;

import io.indexr.exchange.realtime.RealtimeSegmentLoader;
import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;
import io.indexr.server.Instance;
import io.indexr.server.ServerBuilder;
import io.indexr.server.ServerInstance;
import io.indexr.server.config.IndexrConfig;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ServerSegmentManager implements SegmentManager {

  private ServerInstance serverInstance;

  public ServerSegmentManager() throws IOException {
    IndexrConfig config = IndexrConfig.fromResource("config.json");
    serverInstance = ServerBuilder.buildServer(config);
  }

  @Override
  public void start() {
    serverInstance.start();
  }

  @Override
  public void close() {
    serverInstance.close();
  }

  @Override
  public Set<String> tableNames() throws IOException {
    return serverInstance.getServerStatus().getInstances().stream().map(Instance::getName).collect(Collectors.toSet());
  }

  @Override
  public SegmentSchema getSchema(String name) {
    RealtimeSegmentLoader loader = serverInstance.getLoaderManager().getLoader(name);
    return loader == null ? null : loader.schema();
  }

  @Override
  public DrillIndexRTable getTable(IndexRStoragePlugin plugin, String name, IndexRScanSpec spec) throws IOException {
    SegmentSchema segmentSchema = getSchema(name);
    return new DrillIndexRTable(plugin, spec, segmentSchema);
  }

  @Override
  public List<Segment> getSegmentList(String tableName) {
    return serverInstance.getSegmentManager().get(tableName);
  }
}
