package com.alibaba.graphar.graphinfo;

import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.utils.InfoVersion;
import org.junit.Assert;
import org.junit.Test;

public class VertexInfoTest {
  @Test
  public void test1() {
    StdString label = StdString.create("test_vertex");
    long chunkSize = 100;
    InfoVersion infoVersion = InfoVersion.create(1);
    StdString prefix = StdString.create("./");
    VertexInfo vertexInfo = VertexInfo.factory.create(label, chunkSize, infoVersion, prefix);
    Assert.assertEquals(label.toJavaString(), vertexInfo.getLabel().toJavaString());
    Assert.assertEquals(chunkSize, vertexInfo.getChunkSize());
    Assert.assertEquals(prefix.toJavaString(), vertexInfo.getPrefix().toJavaString());
    Assert.assertTrue(infoVersion.eq(vertexInfo.getVersion()));

    // test add property group
  }
}
