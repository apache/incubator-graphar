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

package com.alibaba.graphar.info;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GraphInfo {
    private final String name;
    private final List<VertexInfo> vertexInfos;
    private final List<EdgeInfo> edgeInfos;
    private final String prefix;
    private final Map<String, Integer> vertexLabelToIndex;
    private final Map<EdgeTriple, Integer> edgeTripleToIndex;
    private final VertexInfo version;

    public GraphInfo(
            String name,
            List<VertexInfo> vertexInfos,
            List<EdgeInfo> edgeInfos,
            String prefix,
            VertexInfo version) {
        this.name = name;
        this.vertexInfos = vertexInfos;
        this.edgeInfos = edgeInfos;
        this.prefix = prefix;
        this.vertexLabelToIndex = new HashMap<>(vertexInfos.size());
        for (int i = 0; i < vertexInfos.size(); i++) {
            vertexLabelToIndex.put(vertexInfos.get(i).getLabel(), i);
        }
        this.edgeTripleToIndex = new HashMap<>(edgeInfos.size());
        for (int i = 0; i < edgeInfos.size(); i++) {
            edgeTripleToIndex.put(edgeInfos.get(i).getEdgeTriple(), i);
        }
        this.version = version;
    }
    //
    //    public static GraphInfo load(String graphPath, Configuration conf) throws IOException {
    //        if (graphPath == null) {
    //            conf = new Configuration();
    //        }
    //        Path path = new Path(graphPath);
    //        FileSystem fileSystem = path.getFileSystem(conf);
    //        FSDataInputStream inputStream = fileSystem.open(path);
    //        GraphYamlParser graphYamlParser = new Yaml(new Constructor(GraphYamlParser.class, new
    // LoaderOptions())).load(inputStream);
    //        // TODO: Vertex and edge yaml file path getter
    //
    //    }
    //
    public String getName() {
        return name;
    }

    public int getVertexInfoCount() {
        return vertexInfos.size();
    }

    public List<VertexInfo> getVertexInfos() {
        return vertexInfos;
    }

    public VertexInfo getVertexInfo(String label) {
        return vertexInfos.get(vertexLabelToIndex.get(label));
    }

    public VertexInfo getVertexInfo(int index) {
        return vertexInfos.get(index);
    }

    public int getVertexInfoIndex(String label) {
        return vertexLabelToIndex.get(label);
    }

    public int getEdgeInfoCount() {
        return edgeInfos.size();
    }

    public List<EdgeInfo> getEdgeInfos() {
        return edgeInfos;
    }

    public EdgeInfo getEdgeInfo(EdgeTriple edgeTriple) {
        return edgeInfos.get(edgeTripleToIndex.get(edgeTriple));
    }

    public EdgeInfo getEdgeInfo(int index) {
        return edgeInfos.get(index);
    }

    public int getEdgeInfoIndex(EdgeTriple edgeTriple) {
        return edgeTripleToIndex.get(edgeTriple);
    }

    public String getPrefix() {
        return prefix;
    }

    public List<VertexInfo> getVertexInfos(String label) {
        return vertexInfos;
    }

    public VertexInfo getVersion() {
        return version;
    }
}
