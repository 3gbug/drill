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
package org.apache.drill.store.indexr;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

//@Ignore("requires a segments in local dir")
public class TestIndexRPlugin extends BaseTestQuery {

  @Test
  public void testIndexr() throws Exception {
//    test("use indexr;");
//    test("show tables;");
//    test("describe campaign");

    // 19 is the limit for drill transform in into hashjoin!

    test("select campaign_id, sum(clicks), sum(impressions) from indexr.campaign " +
            "where `date` between 9 and 1000000000 and 0 < user_id " +
            "and campaign_id in (0, 59855, 54638,55917, 11, 44, 55)  " +
            "group by campaign_id order by sum(impressions) desc limit 10");
    test("select campaign_id, sum(clicks), sum(impressions) from indexr.campaign where campaign_id < 59855 group by campaign_id order by sum(impressions) desc limit 10");
  }

  @Test
  public void testEmbeddedServer() throws Exception {
    test("select * from indexr.test_realtime limit 10");
  }

}