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

/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.writers.builder;

import static com.alibaba.graphar.graphinfo.GraphInfoTest.root;

import com.alibaba.graphar.graphinfo.EdgeInfo;
import com.alibaba.graphar.stdcxx.StdSharedPtr;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.types.ValidateLevel;
import com.alibaba.graphar.util.Yaml;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

public class EdgesBuilderTest {
    @Test
    public void test1() {
        // construct edge builder
        String edgeMetaFile = root + "/ldbc_sample/parquet/" + "person_knows_person.edge.yml";
        StdSharedPtr<Yaml> edgeMeta = Yaml.loadFile(StdString.create(edgeMetaFile)).value();
        EdgeInfo edgeInfo = EdgeInfo.load(edgeMeta).value();
        long verticesNum = 903;
        EdgesBuilder builder =
                EdgesBuilder.factory.create(
                        edgeInfo,
                        StdString.create("/tmp/"),
                        AdjListType.ordered_by_dest,
                        verticesNum);

        // get & set validate level
        Assert.assertEquals(ValidateLevel.no_validate, builder.getValidateLevel());
        builder.setValidateLevel(ValidateLevel.strong_validate);
        Assert.assertEquals(ValidateLevel.strong_validate, builder.getValidateLevel());

        // check different validate levels
        Edge e = Edge.factory.create(0, 1);
        e.addProperty(StdString.create("creationDate"), 2020);
        Assert.assertTrue(builder.addEdge(e, ValidateLevel.no_validate).ok());
        Assert.assertTrue(builder.addEdge(e, ValidateLevel.weak_validate).ok());
        Assert.assertTrue(builder.addEdge(e, ValidateLevel.strong_validate).isTypeError());
        e.addProperty(StdString.create("invalid_name"), StdString.create("invalid_value"));
        Assert.assertTrue(builder.addEdge(e).isKeyError());

        // clear edges
        builder.clear();
        Assert.assertEquals(0, builder.getNum());

        // add edges
        String fileName = root + "/ldbc_sample/person_knows_person_0_0.csv";
        StdString stdStrPropertyName = StdString.create("creationDate");
        String line;
        Map<String, Long> mapping = new HashMap<>();
        long cnt = 0;
        long lines = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            br.readLine(); // read the first line but not use

            while ((line = br.readLine()) != null) { // read the remaining lines
                lines++;
                String[] vals = line.split("\\|"); // split by pipe delimiter
                long src = 0;
                long dst = 0;
                for (int i = 0; i < 3; i++) {
                    if (i == 0) {
                        if (!mapping.containsKey(vals[i])) {
                            mapping.put(vals[i], cnt++);
                        }
                        src = mapping.get(vals[i]);
                    } else if (i == 1) {
                        if (!mapping.containsKey(vals[i])) {
                            mapping.put(vals[i], cnt++);
                        }
                        dst = mapping.get(vals[i]);
                    } else {
                        Edge innerEdge = Edge.factory.create(src, dst);
                        e.addProperty(stdStrPropertyName, StdString.create(vals[i]));
                        Assert.assertTrue(builder.addEdge(innerEdge).ok());
                    }
                }
            }
        } catch (IOException IOE) {
            IOE.printStackTrace();
        }

        // check the number of edges in builder
        Assert.assertEquals(lines, builder.getNum());

        // dump to files
        Assert.assertTrue(builder.dump().ok());

        // can not add new edges after dumping
        Assert.assertTrue(builder.addEdge(e).isInvalid());
    }
}
