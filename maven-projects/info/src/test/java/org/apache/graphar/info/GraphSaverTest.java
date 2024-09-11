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
