package org.apache.graphar.info;

import java.io.IOException;
import org.apache.graphar.info.loader.Loader;
import org.apache.graphar.info.loader.LocalYamlLoader;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class LoaderTest {

    @BeforeClass
    public static void init() {
        TestUtil.checkTestData();
    }

    @AfterClass
    public static void clean() {}

    @Test
    public void testLoadGraph() {
        final String GRAPH_PATH = TestUtil.getTestData() + "/ldbc_sample/csv/ldbc_sample.graph.yml";
        Loader loader = new LocalYamlLoader();
        try {
            GraphInfo graphInfo = loader.loadGraph(GRAPH_PATH);
            Assert.assertNotNull(graphInfo);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLoadVertex() {
        final String VERTEX_PATH = TestUtil.getTestData() + "/ldbc_sample/csv/person.vertex.yml";
        Loader loader = new LocalYamlLoader();
        try {
            VertexInfo vertexInfo = loader.loadVertex(VERTEX_PATH);
            Assert.assertNotNull(vertexInfo);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testLoadEdge() {
        final String EDGE_PATH =
                TestUtil.getTestData() + "/ldbc_sample/csv/person_knows_person.edge.yml";
        Loader loader = new LocalYamlLoader();
        try {
            EdgeInfo edgeInfo = loader.loadEdge(EDGE_PATH);
            Assert.assertNotNull(edgeInfo);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
