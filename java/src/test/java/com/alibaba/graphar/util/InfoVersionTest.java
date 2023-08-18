package com.alibaba.graphar.util;

import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.stdcxx.StdVector;
import org.junit.Test;
import org.junit.Assert;

public class InfoVersionTest {
  @Test
  public void test1() {
    InfoVersion infoVersion = InfoVersion.create(1);
    Assert.assertEquals(1, infoVersion.version());
    Assert.assertEquals(0, infoVersion.userDefineTypes().size());
    Assert.assertEquals("gar/v1", infoVersion.toStdString().toJavaString());
    Assert.assertTrue(infoVersion.checkType(StdString.create("int32")));
    Assert.assertFalse(infoVersion.checkType(StdString.create("date32")));

    StdVector.Factory<StdString> stdStringVecFactory =
        StdVector.getStdVectorFactory("std::vector<std::string>");
    StdVector<StdString> usrDefineTypeVec = stdStringVecFactory.create();
    usrDefineTypeVec.push_back(StdString.create("t1"));
    usrDefineTypeVec.push_back(StdString.create("t2"));
    InfoVersion infoVersion2 = InfoVersion.factory.create(1, usrDefineTypeVec);
    Assert.assertEquals(1, infoVersion2.version());
    Assert.assertTrue(usrDefineTypeVec.eq(infoVersion2.userDefineTypes()));
    Assert.assertEquals("gar/v1 (t1,t2)", infoVersion2.toStdString().toJavaString());
    Assert.assertTrue(infoVersion2.checkType(StdString.create("t1")));
  }
}
