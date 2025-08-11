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

package org.apache.graphar.vertices;

import static org.apache.graphar.graphinfo.GraphInfoTest.root;

import org.apache.graphar.graphinfo.GraphInfo;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.apache.graphar.util.Result;
import org.junit.Assert;
import org.junit.Test;

public class VerticesCollectionTest {
    @Test
    public void test1() {
        // read file and construct graph info
        String path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
        Result<StdSharedPtr<GraphInfo>> maybeGraphInfo = GraphInfo.load(path);
        Assert.assertTrue(maybeGraphInfo.status().ok());
        StdSharedPtr<GraphInfo> graphInfo = maybeGraphInfo.value();

        // construct vertices collection
        StdString label = StdString.create("person");
        StdString property = StdString.create("firstName");
        StdString stdStrId = StdString.create("id");
        Result<StdSharedPtr<VerticesCollection>> maybeVerticesCollection =
                GrapharStaticFunctions.INSTANCE.constructVerticesCollection(graphInfo, label);
        Assert.assertFalse(maybeVerticesCollection.hasError());
        StdSharedPtr<VerticesCollection> vertices = maybeVerticesCollection.value();
        VertexIter it = vertices.get().begin();
        for (Vertex vertex : vertices.get()) {
            // access data reference through vertex
            Assert.assertEquals(
                    it.<Long>property(stdStrId, 1L).value(),
                    vertex.<Long>property(stdStrId, 1L).value());
            Assert.assertEquals(
                    it.<StdString>property(property, property).value().toJavaString(),
                    vertex.<StdString>property(property, property).value().toJavaString());
            it.inc();
        }
    }
}
