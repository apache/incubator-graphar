package org.apache.graphar.info;

import java.io.IOException;
import org.apache.graphar.info.loader.GraphLoader;
import org.apache.graphar.info.loader.LocalYamlGraphLoader;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GraphLoaderTest {

    @BeforeClass
    public static void init() {
        TestUtil.checkTestData();
    }

    @AfterClass
    public static void clean() {}

    @Test
    public void testLoad() {
        final GraphLoader graphLoader = new LocalYamlGraphLoader();
        final String GRAPH_PATH = TestUtil.getLdbcSampleGraphPath();
        try {
            final GraphInfo graphInfo = graphLoader.load(GRAPH_PATH);
            Assert.assertNotNull(graphInfo);
            Assert.assertNotNull(graphInfo.getEdgeInfos());
            Assert.assertEquals(1, graphInfo.getEdgeInfos().size());
            for (EdgeInfo edgeInfo : graphInfo.getEdgeInfos()) {
                Assert.assertNotNull(edgeInfo.getConcat());
            }
            Assert.assertNotNull(graphInfo.getVertexInfos());
            Assert.assertEquals(1, graphInfo.getVertexInfos().size());
            for (VertexInfo vertexInfo : graphInfo.getVertexInfos()) {
                Assert.assertNotNull(vertexInfo.getType());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
