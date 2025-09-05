/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.graphar.info;

import java.io.File;
import org.apache.graphar.info.saver.GraphSaver;
import org.apache.graphar.info.saver.LocalYamlGraphSaver;
import org.junit.Assert;
import org.junit.Test;

public class GraphSaverTest {

    @Test
    public void testSave() {
        final String LDBC_SAMPLE_SAVE_DIR = TestUtil.SAVE_DIR + "/ldbc_sample/";
        final GraphSaver graphSaver = new LocalYamlGraphSaver();
        final GraphInfo graphInfo = TestUtil.getLdbcSampleDataSet();
        try {
            graphSaver.save(LDBC_SAMPLE_SAVE_DIR, graphInfo);
            Assert.assertTrue(
                    new File(LDBC_SAMPLE_SAVE_DIR + "/" + graphInfo.getName() + ".graph.yaml")
                            .exists());
            for (VertexInfo vertexInfo : graphInfo.getVertexInfos()) {
                Assert.assertTrue(
                        new File(LDBC_SAMPLE_SAVE_DIR + "/" + vertexInfo.getType() + ".vertex.yaml")
                                .exists());
            }
            for (EdgeInfo edgeInfo : graphInfo.getEdgeInfos()) {
                Assert.assertTrue(
                        new File(LDBC_SAMPLE_SAVE_DIR + "/" + edgeInfo.getConcat() + ".edge.yaml")
                                .exists());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
