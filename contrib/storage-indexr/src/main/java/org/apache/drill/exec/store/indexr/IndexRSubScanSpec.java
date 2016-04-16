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
import io.indexr.segment.rc.RCOperator;

public class IndexRSubScanSpec {
  @JsonProperty("tableName")
  public final String tableName;

  @JsonProperty("parallelization")
  public final Integer parallelization;

  @JsonProperty("parallelizationIndex")
  public final Integer parallelizationIndex;

  @JsonProperty("rsFilter")
  public final RCOperator rsFilter;

  @JsonCreator
  public IndexRSubScanSpec(@JsonProperty("tableName") String tableName, //
                           @JsonProperty("parallelization") Integer parallelization, //
                           @JsonProperty("parallelizationIndex") Integer parallelizationIndex,//
                           @JsonProperty("rsFilter") RCOperator rsFilter) {
    this.tableName = tableName;
    this.parallelization = parallelization;
    this.parallelizationIndex = parallelizationIndex;
    this.rsFilter = rsFilter;
  }

  @Override
  public String toString() {
    return "IndexRSubScanSpec{" +
      "tableName='" + tableName + '\'' +
      ", parallelization=" + parallelization +
      ", parallelizationIndex=" + parallelizationIndex +
      ", rsFilter=" + rsFilter +
      '}';
  }
}
