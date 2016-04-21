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

import io.indexr.segment.Segment;
import io.indexr.segment.SegmentSchema;
import io.indexr.segment.pack.DPSegment;
import io.indexr.util.ExtraStringUtil;
import io.indexr.util.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FakeSegmentManager {
  private static final Logger log = LoggerFactory.getLogger(FakeSegmentManager.class);

  private String dataDir;

  private Map<String, FakeTable> tables;

  public FakeSegmentManager(String dataDir) throws IOException {
    this.dataDir = dataDir;

    tables = new HashMap<>();
    init();
    showTables();
  }

  private void init() throws IOException {
    tablePaths().forEach(tablePath -> {
      try {
        SegmentSchema schema = JsonUtil.load(tablePath.resolve("schema.json"), SegmentSchema.class);
        Map<String, Segment> idToSegment = Files.list(tablePath)//
          .filter(p -> Files.isDirectory(p) && pathLastName(p).startsWith("segment_"))//
          .map(p -> DPSegment.fromPath(p.toAbsolutePath().toString(), false))//
          .collect(Collectors.toMap(Segment::name, p -> p));
        tables.put(schema.name,//
          new FakeTable(schema, Collections.unmodifiableList(new ArrayList<>(idToSegment.values()))));
      } catch (IOException e) {
        log.warn("", e);
      }
    });
  }

  private void showTables() {
    System.out.println("FakeSegmentManager tables:");
    tables.values().forEach(table -> {
      System.out.println("======================");
      System.out.println(String.format("table: %s", table.schema.name));
      System.out.println("segments:");
      table.segments.forEach(segment -> {
        System.out.println(segment.toString());
      });
    });
  }

  private Stream<Path> tablePaths() throws IOException {
    return Files.list(Paths.get(dataDir)).filter(p -> Files.isDirectory(p) && pathLastName(p).startsWith("table_"));
  }

  private static String pathLastName(Path p) {
    return ExtraStringUtil.trim(p.getFileName().toString(), "/");
  }

  public Set<String> tableNames() throws IOException {
    return tables.keySet();
  }

  public SegmentSchema getSchema(String name) {
    FakeTable table = tables.get(name.toLowerCase());
    if (table == null) {
      return null;
    }
    return table.schema;
  }

  public DrillIndexRTable getTable(IndexRStoragePlugin plugin, String name, IndexRScanSpec spec) throws IOException {
    FakeTable table = tables.get(name.toLowerCase());
    if (table == null) {
      return null;
    }
    return new DrillIndexRTable(plugin, spec, table.schema);
  }

  public List<Segment> getSegmentList(String tableName) {
    FakeTable table = tables.get(tableName.toLowerCase());
    return table == null ? Collections.emptyList() : table.segments;
  }

  public static class FakeTable {
    public final SegmentSchema schema;

    public final List<Segment> segments;

    public FakeTable(SegmentSchema schema, List<Segment> segments) {
      this.schema = schema;
      this.segments = segments;
    }
  }

}