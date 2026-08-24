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

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.graphar.info.yaml.GraphYaml;
import org.junit.Assert;
import org.junit.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

public class GraphInfoMetadataTest {

    @Test
    public void testLabelsAndExtraInfoRoundTrip() {
        GraphInfo graphInfo =
                new GraphInfo(
                        "metadata_graph",
                        List.of(),
                        List.of(),
                        URI.create("file:///tmp/metadata_graph/"),
                        "gar/v1",
                        List.of("production", "ldbc"),
                        Map.of("category", "test graph"));

        String dumped = graphInfo.dump();
        Yaml yaml = new Yaml(new Constructor(GraphYaml.class, new LoaderOptions()));
        GraphYaml graphYaml = yaml.load(dumped);

        Assert.assertEquals(List.of("production", "ldbc"), graphYaml.getLabels());
        Assert.assertEquals(1, graphYaml.getExtra_info().size());
        Assert.assertEquals("category", graphYaml.getExtra_info().get(0).getKey());
        Assert.assertEquals("test graph", graphYaml.getExtra_info().get(0).getValue());
    }
}
