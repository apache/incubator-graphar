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

/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.util;

import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.stdcxx.StdVector;
import org.junit.Assert;
import org.junit.Test;

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
