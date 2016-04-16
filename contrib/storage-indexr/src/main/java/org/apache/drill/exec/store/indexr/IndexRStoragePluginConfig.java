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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.drill.common.logical.StoragePluginConfigBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonTypeName(IndexRStoragePluginConfig.NAME)
public class IndexRStoragePluginConfig extends StoragePluginConfigBase {
  private static final Logger log = LoggerFactory.getLogger(IndexRStoragePluginConfig.class);

  public static final String NAME = "indexr";

  private final String dataDir;

  private final Integer scanThreadsPerNode;

  private final boolean enableRSFilter;

  @JsonCreator
  public IndexRStoragePluginConfig(@JsonProperty("dataDir") String dataDir,//
                                   @JsonProperty("scanThreadsPerNode") Integer scanThreadsPerNode,//
                                   @JsonProperty("enableRSFilter") Boolean enableRSFilter) {
    this.dataDir = dataDir;
    this.scanThreadsPerNode = scanThreadsPerNode != null && scanThreadsPerNode > 0 ? scanThreadsPerNode : 1;
    this.enableRSFilter = enableRSFilter != null ? enableRSFilter : true;
  }

  @JsonProperty("dataDir")
  public String getDataDir() {
    return dataDir;
  }

  @JsonProperty("scanThreadsPerNode")
  public int getScanThreadsPerNode() {
    return scanThreadsPerNode;
  }

  @JsonProperty("enableRSFilter")
  public boolean getEnableRSFilter() {
    return enableRSFilter;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexRStoragePluginConfig that = (IndexRStoragePluginConfig) o;
    return dataDir != null ? dataDir.equals(that.dataDir) : that.dataDir == null;
  }

  @Override
  public int hashCode() {
    return dataDir != null ? dataDir.hashCode() : 0;
  }
}
