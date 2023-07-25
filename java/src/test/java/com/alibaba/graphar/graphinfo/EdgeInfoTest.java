package com.alibaba.graphar.graphinfo;

import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.utils.InfoVersion;
import org.junit.Assert;
import org.junit.Test;

public class EdgeInfoTest {
  @Test
  public void test1() {
    StdString srcLabel = StdString.create("person");
    StdString edgeLabel = StdString.create("knows");
    StdString dstLabel = StdString.create("person");
    long chunkSize = 100;
    long srcChunkSize = 100;
    long dstChunkSize = 100;
    boolean directed = true;
    InfoVersion infoVersion = InfoVersion.create(1);
    StdString prefix = StdString.create("");
    EdgeInfo edgeInfo =
        EdgeInfo.factory.create(
            srcLabel,
            edgeLabel,
            dstLabel,
            chunkSize,
            srcChunkSize,
            dstChunkSize,
            directed,
            infoVersion,
            prefix);
    Assert.assertEquals(srcLabel.toJavaString(), edgeInfo.getSrcLabel().toJavaString());
    Assert.assertEquals(edgeLabel.toJavaString(), edgeInfo.getEdgeLabel().toJavaString());
    Assert.assertEquals(dstLabel.toJavaString(), edgeInfo.getDstLabel().toJavaString());
    Assert.assertEquals(chunkSize, edgeInfo.getChunkSize());
    Assert.assertEquals(srcChunkSize, edgeInfo.getSrcChunkSize());
    Assert.assertEquals(dstChunkSize, edgeInfo.getDstChunkSize());
    Assert.assertEquals(directed, edgeInfo.isDirected());
    Assert.assertEquals(
        srcLabel.toJavaString()
            + "_"
            + edgeLabel.toJavaString()
            + "_"
            + dstLabel.toJavaString()
            + "/",
        edgeInfo.getPrefix().toJavaString());
    Assert.assertTrue(infoVersion.eq(edgeInfo.getVersion()));
  }
}
