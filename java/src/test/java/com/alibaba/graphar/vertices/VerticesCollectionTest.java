/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.vertices;

import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.util.GrapharStaticFunctions;
import com.alibaba.graphar.util.Result;
import org.junit.Assert;
import org.junit.Test;

import static com.alibaba.graphar.graphinfo.GraphInfoTest.root;

public class VerticesCollectionTest {
  @Test
  public void test1() {
    // read file and construct graph info
    String path = root + "/ldbc_sample/parquet/ldbc_sample.graph.yml";
    Result<GraphInfo> maybeGraphInfo = GraphInfo.load(path);
    Assert.assertTrue(maybeGraphInfo.status().ok());
    GraphInfo graphInfo = maybeGraphInfo.value();

    // construct vertices collection
    StdString label = StdString.create("person");
    StdString property = StdString.create("firstName");
    StdString stdStrId = StdString.create("id");
    Result<VerticesCollection> maybeVerticesCollection =
            GrapharStaticFunctions.INSTANCE.constructVerticesCollection(graphInfo, label);
    Assert.assertFalse(maybeVerticesCollection.hasError());
    VerticesCollection vertices = maybeVerticesCollection.value();
    for (VertexIter it = vertices.begin(); !it.eq(vertices.end()); it.inc()) {
      // access data through iterator directly
      VertexIterGen itGen = (VertexIterGen) it;
      System.out.println(
              it.id()
                      + ", id="
                      + it.<Long>property(stdStrId, 1L).value()
                      + ", firstName="
                      + it.property(property, property).value().toJavaString());
      // access data through vertex
      Vertex vertex = it.get();
      System.out.println(
              vertex.id()
                      + ", id="
                      + vertex.<Long>property(stdStrId, 1L).value()
                      + ", firstName="
                      + vertex.<StdString>property(property, property).value().toJavaString());
      // access data reference through vertex
      Assert.assertEquals(
              it.<Long>property(stdStrId, 1L).value(), vertex.<Long>property(stdStrId, 1L).value());
      Assert.assertEquals(
              it.<StdString>property(property, property).value().toJavaString(),
              vertex.<StdString>property(property, property).value().toJavaString());
    }
  }
}
