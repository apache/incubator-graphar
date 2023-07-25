package com.alibaba.graphar.utils;

import com.alibaba.graphar.stdcxx.StdString;
import org.junit.Test;
import org.junit.Assert;

public class InfoVersionTest {
  @Test
  public void test1() {
    int versionRes = 1;
    String toStringRes = "gar/v1";
    int usrDefineTypesRes = 0;

    InfoVersion infoVersion = InfoVersion.create(1);
    Assert.assertEquals(versionRes, infoVersion.version());
    Assert.assertEquals(toStringRes, infoVersion.toStdString().toJavaString());
    Assert.assertTrue(infoVersion.checkType(StdString.create("string")));
    Assert.assertEquals(usrDefineTypesRes, infoVersion.userDefineTypes().size());
  }
}
