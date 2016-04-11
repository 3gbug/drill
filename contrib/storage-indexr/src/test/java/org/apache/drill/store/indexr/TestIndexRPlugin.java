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
package org.apache.drill.store.indexr;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

//@Ignore("requires a segments in local dir")
public class TestIndexRPlugin extends BaseTestQuery {

    @Test
    public void testIndexr() throws Exception {
        //test("use indexr;");
        //test("show tables;");
        //test("describe campaign");

        // 19 is the limit for drill transform in into hashjoin!

        //test("select campaign_id, sum(impressions), sum(clicks), sum(cost) from indexr.campaign where campaign_id in (55986,56548,56549,57005,57008,57057,57271,57512,57828,57860,57862,57969,58176,58239,58257,58500,58531,58576,58660,58761,58805,58939,59194,59329,59441,59553,59643,59698,59704,59948,59956,59959,59960,59961,60182,60200,60204,60313,60430,60489,60567,60569,60640,60642,60645,60698,60724,60769,60876,60887,60943,60983,61005,61014,61068,61073,61079,61132,61141,61178,61185,61190,61191) and channel_id > 10 group by campaign_id;");
        //test("select campaign_id, sum(impressions), sum(clicks), sum(cost) from indexr.campaign where campaign_id in (-100, 55) group by campaign_id order by campaign_id asc limit 100;");
        test("select channel_id, sum(impressions), sum(clicks), sum(cost) from indexr.campaign where channel_id in (44, 300, 62, 10013)  group by channel_id order by channel_id asc limit 100;");
        //test("select campaign_id, sum(impressions), sum(clicks), sum(cost) from indexr.campaign where campaign_id in (55986,56548,56549,57005,57008,57271,57512,57828,57860) group by campaign_id order by campaign_id asc limit 100;");
        //test("select count(*) from indexr.campaign;");
        //test("select campaign_id, sum(impressions), avg(cost), max(clicks) from indexr.campaign group by campaign_id order by sum(impressions) desc limit 10;");
    }

}
