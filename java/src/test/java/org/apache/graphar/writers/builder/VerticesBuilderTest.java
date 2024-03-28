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

package org.apache.graphar.writers.builder;

import static org.apache.graphar.graphinfo.GraphInfoTest.root;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.graphar.graphinfo.VertexInfo;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.types.ValidateLevel;
import org.apache.graphar.util.Yaml;
import org.junit.Assert;
import org.junit.Test;

public class VerticesBuilderTest {
    @Test
    public void test1() {
        // construct vertex builder
        String vertexMetaFile = root + "/ldbc_sample/parquet/" + "person.vertex.yml";
        StdSharedPtr<Yaml> vertexMeta = Yaml.loadFile(StdString.create(vertexMetaFile)).value();
        VertexInfo vertexInfo = VertexInfo.load(vertexMeta).value();
        long startIndex = 0L;
        VerticesBuilder builder =
                VerticesBuilder.factory.create(vertexInfo, StdString.create("/tmp/"), startIndex);

        // get & set validate level
        Assert.assertEquals(ValidateLevel.no_validate, builder.getValidateLevel());
        builder.setValidateLevel(ValidateLevel.strong_validate);
        Assert.assertEquals(ValidateLevel.strong_validate, builder.getValidateLevel());

        // check different validate levels
        Vertex v = Vertex.factory.create();
        v.addProperty(StdString.create("id"), StdString.create("id_of_string"));
        Assert.assertTrue(builder.addVertex(v, 0, ValidateLevel.no_validate).ok());
        Assert.assertTrue(builder.addVertex(v, 0, ValidateLevel.weak_validate).ok());
        builder.addVertex(v, -2, ValidateLevel.weak_validate);
        Assert.assertTrue(builder.addVertex(v, -2, ValidateLevel.weak_validate).isIndexError());
        Assert.assertTrue(builder.addVertex(v, 0, ValidateLevel.strong_validate).isTypeError());
        v.addProperty(StdString.create("invalid_name"), StdString.create("invalid_value"));
        Assert.assertTrue(builder.addVertex(v, 0).isKeyError());

        // clear vertices
        builder.clear();
        Assert.assertEquals(0, builder.getNum());

        // add vertices
        String fileName = root + "/ldbc_sample/person_0_0.csv";
        int m = 4;
        int lines = 0;
        List<StdString> names = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            String line = br.readLine(); // read the first line
            String[] values = line.split("\\|"); // split by pipe delimiter
            for (int i = 0; i < m; i++) {
                names.add(StdString.create(values[i])); // add the name to the list
            }

            while ((line = br.readLine()) != null) { // read the remaining lines
                lines++;
                values = line.split("\\|"); // split by pipe delimiter
                Vertex innerVertex = Vertex.factory.create();
                for (int i = 0; i < m; i++) {
                    if (i == 0) {
                        long x = Long.parseLong(values[i]); // parse the value as long
                        innerVertex.addProperty(names.get(i), x); // add the property to the vertex
                    } else {
                        StdString tempStdStrValue = StdString.create(values[i]);
                        innerVertex.addProperty(
                                names.get(i), tempStdStrValue); // add the property to the vertex
                        tempStdStrValue.delete();
                    }
                }
                Assert.assertTrue(builder.addVertex(innerVertex).ok());
                innerVertex.delete();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // check the number of vertices in builder
        Assert.assertEquals(lines, builder.getNum());

        // dump to files
        Assert.assertTrue(builder.dump().ok());

        // can not add new vertices after dumping
        Assert.assertTrue(builder.addVertex(v).isInvalid());
    }
}
