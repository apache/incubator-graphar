package com.alibaba.graphar.graphinfo;

import com.alibaba.graphar.utils.InfoVersion;
import org.junit.Assert;
import org.junit.Test;

public class GraphInfoTest {
  @Test
  public void test1() {
    String graphName = "test_graph";
    String prefix = "test_prefix";
    InfoVersion version = InfoVersion.create(1);
    GraphInfo graphInfo = GraphInfo.create(graphName, version, prefix);
    Assert.assertEquals(graphInfo.getName().toJavaString(), graphName);
    Assert.assertEquals(graphInfo.getPrefix().toJavaString(), prefix);
    Assert.assertTrue(graphInfo.getInfoVersion().eq(version));

    // test add vertex and get vertex info
  }
}
