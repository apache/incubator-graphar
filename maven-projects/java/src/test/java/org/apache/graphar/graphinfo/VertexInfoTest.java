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

package org.apache.graphar.graphinfo;

import java.io.File;
import org.apache.graphar.stdcxx.StdSharedPtr;
import org.apache.graphar.stdcxx.StdString;
import org.apache.graphar.stdcxx.StdVector;
import org.apache.graphar.types.FileType;
import org.apache.graphar.util.GrapharStaticFunctions;
import org.apache.graphar.util.Result;
import org.apache.graphar.util.Status;
import org.junit.Assert;
import org.junit.Test;

public class VertexInfoTest {
    @Test
    public void test1() {
        StdString label = StdString.create("test_vertex");
        long chunkSize = 100;
        StdVector.Factory<StdSharedPtr<PropertyGroup>> propertyGroupVecFactory =
                StdVector.getStdVectorFactory(
                        "std::vector<std::shared_ptr<graphar::PropertyGroup>>");
        StdVector<StdSharedPtr<PropertyGroup>> propertyGroupStdVector =
                propertyGroupVecFactory.create();
        StdString prefix = StdString.create("test_vertex/");
        StdSharedPtr<VertexInfo> vertexInfoStdSharedPtr =
                GrapharStaticFunctions.INSTANCE.createVertexInfo(
                        label, chunkSize, propertyGroupStdVector, prefix);
        VertexInfo vertexInfo = vertexInfoStdSharedPtr.get();
        Assert.assertTrue(label.eq(vertexInfo.getLabel()));
        Assert.assertEquals(chunkSize, vertexInfo.getChunkSize());
        Assert.assertEquals(label.toJavaString() + "/", vertexInfo.getPrefix().toJavaString());

        // test add property group
        Property property = Property.factory.create(StdString.create("id"));
        property.type(GrapharStaticFunctions.INSTANCE.stringType());
        StdVector.Factory<Property> propertyFactory =
                StdVector.getStdVectorFactory("std::vector<graphar::Property>");
        StdVector<Property> propertyStdVector = propertyFactory.create();
        propertyStdVector.push_back(property);
        StdSharedPtr<PropertyGroup> propertyGroup =
                GrapharStaticFunctions.INSTANCE.createPropertyGroup(
                        propertyStdVector, FileType.CSV, StdString.create("test_vertex_pg1/"));
        StdSharedPtr<PropertyGroup> propertyGroup2 =
                GrapharStaticFunctions.INSTANCE.createPropertyGroup(
                        propertyStdVector, FileType.PARQUET, StdString.create("test_vertex_pg1/"));
        Assert.assertEquals(0, vertexInfo.getPropertyGroups().size());
        Result<StdSharedPtr<VertexInfo>> stdSharedPtrResult =
                vertexInfo.addPropertyGroup(propertyGroup);
        Assert.assertTrue(stdSharedPtrResult.status().ok());
        vertexInfo = stdSharedPtrResult.value().get();
        // same property group can not be added twice
        stdSharedPtrResult = vertexInfo.addPropertyGroup(propertyGroup);
        Assert.assertTrue(stdSharedPtrResult.status().isInvalid());
        // same property can not be put in different property group
        Assert.assertTrue(vertexInfo.addPropertyGroup(propertyGroup2).status().isInvalid());
        Assert.assertEquals(1, vertexInfo.getPropertyGroups().size());

        Property property2 = Property.factory.create(StdString.create("name"));
        property2.type(GrapharStaticFunctions.INSTANCE.stringType());
        StdVector<Property> propertyStdVector2 = propertyFactory.create();
        propertyStdVector2.push_back(property2);
        StdSharedPtr<PropertyGroup> propertyGroup3 =
                GrapharStaticFunctions.INSTANCE.createPropertyGroup(
                        propertyStdVector2, FileType.CSV, StdString.create("test_vertex_pg2/"));
        stdSharedPtrResult = vertexInfo.addPropertyGroup(propertyGroup3);
        Assert.assertTrue(stdSharedPtrResult.status().ok());
        vertexInfo = stdSharedPtrResult.value().get();

        // test get property meta
        StdString notExistKey = StdString.create("not_exist_key");
        Assert.assertEquals(property.is_primary(), vertexInfo.isPrimaryKey(property.name()));
        Assert.assertTrue(vertexInfo.hasPropertyGroup(propertyGroup));
        Assert.assertFalse(vertexInfo.hasPropertyGroup(propertyGroup2));
        StdSharedPtr<PropertyGroup> propertyGroupResult =
                vertexInfo.getPropertyGroup(property.name());
        Assert.assertNotNull(propertyGroupResult.get());
        Assert.assertTrue(
                property.name().eq(propertyGroupResult.get().getProperties().get(0).name()));
        Assert.assertNull(vertexInfo.getPropertyGroup(notExistKey).get());

        // test get dir path
        String expectedDirPath =
                vertexInfo.getPrefix().toJavaString()
                        + propertyGroup.get().getPrefix().toJavaString();
        Result<StdString> maybeDirPath = vertexInfo.getPathPrefix(propertyGroup);
        Assert.assertFalse(maybeDirPath.hasError());
        Assert.assertEquals(expectedDirPath, maybeDirPath.value().toJavaString());
        // test get file path
        Result<StdString> maybePath = vertexInfo.getFilePath(propertyGroup, 0);
        Assert.assertFalse(maybePath.hasError());
        Assert.assertEquals(expectedDirPath + "chunk0", maybePath.value().toJavaString());
        // vertex count file path
        Result<StdString> maybePath2 = vertexInfo.getVerticesNumFilePath();
        Assert.assertFalse(maybePath2.hasError());
        Assert.assertEquals(
                vertexInfo.getPrefix().toJavaString() + "vertex_count",
                maybePath2.value().toJavaString());
        // test save
        StdString savePath = StdString.create("/tmp/gar-java-tmp-file");
        Status save = vertexInfo.save(savePath);
        Assert.assertTrue(save.ok());
        File tempFile = new File(savePath.toJavaString());
        Assert.assertTrue(tempFile.exists());
    }
}
