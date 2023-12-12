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


package com.alibaba.graphar.graphinfo;

import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.stdcxx.StdVector;
import com.alibaba.graphar.types.DataType;
import com.alibaba.graphar.types.FileType;
import com.alibaba.graphar.types.Type;
import com.alibaba.graphar.util.InfoVersion;
import com.alibaba.graphar.util.Result;
import java.io.File;
import org.junit.Assert;
import org.junit.Test;

public class VertexInfoTest {
    @Test
    public void test1() {
        StdString label = StdString.create("test_vertex");
        long chunkSize = 100;
        InfoVersion infoVersion = InfoVersion.create(1);
        VertexInfo vertexInfo = VertexInfo.factory.create(label, chunkSize, infoVersion);
        Assert.assertTrue(label.eq(vertexInfo.getLabel()));
        Assert.assertEquals(chunkSize, vertexInfo.getChunkSize());
        Assert.assertEquals(label.toJavaString() + "/", vertexInfo.getPrefix().toJavaString());
        Assert.assertTrue(infoVersion.eq(vertexInfo.getVersion()));

        // test add property group
        Property property = Property.factory.create();
        property.setName(StdString.create("id"));
        property.setType(DataType.factory.create(Type.INT32));
        property.setPrimary(true);
        StdVector.Factory<Property> propertyFactory =
                StdVector.getStdVectorFactory("std::vector<GraphArchive::Property>");
        StdVector<Property> propertyStdVector = propertyFactory.create();
        propertyStdVector.push_back(property);
        PropertyGroup propertyGroup = PropertyGroup.factory.create(propertyStdVector, FileType.CSV);
        PropertyGroup propertyGroup2 =
                PropertyGroup.factory.create(propertyStdVector, FileType.PARQUET);
        Assert.assertEquals(0, vertexInfo.getPropertyGroups().size());
        Assert.assertTrue(vertexInfo.addPropertyGroup(propertyGroup).ok());
        // same property group can not be added twice
        Assert.assertTrue(vertexInfo.addPropertyGroup(propertyGroup).isInvalid());
        // same property can not be put in different property group
        Assert.assertTrue(vertexInfo.addPropertyGroup(propertyGroup2).isInvalid());
        Assert.assertEquals(1, vertexInfo.getPropertyGroups().size());

        Property property2 = Property.factory.create();
        property2.setName(StdString.create("name"));
        property2.setType(DataType.factory.create(Type.STRING));
        property2.setPrimary(false);
        StdVector<Property> propertyStdVector2 = propertyFactory.create();
        propertyStdVector2.push_back(property2);
        PropertyGroup propertyGroup3 =
                PropertyGroup.factory.create(propertyStdVector2, FileType.CSV);
        Assert.assertTrue(vertexInfo.addPropertyGroup(propertyGroup3).ok());

        // test get property meta
        StdString notExistKey = StdString.create("not_exist_key");
        Assert.assertTrue(
                property.getType().eq(vertexInfo.getPropertyType(property.getName()).value()));
        Assert.assertEquals(
                property.isPrimary(), vertexInfo.isPrimaryKey(property.getName()).value());
        Assert.assertTrue(vertexInfo.isPrimaryKey(notExistKey).status().isKeyError());
        Assert.assertTrue(vertexInfo.containPropertyGroup(propertyGroup));
        Assert.assertFalse(vertexInfo.containPropertyGroup(propertyGroup2));
        Result<PropertyGroup> propertyGroupResult = vertexInfo.getPropertyGroup(property.getName());
        Assert.assertFalse(propertyGroupResult.hasError());
        Assert.assertTrue(
                property.getName()
                        .eq(propertyGroupResult.value().getProperties().get(0).getName()));
        Assert.assertTrue(vertexInfo.getPropertyGroup(notExistKey).status().isKeyError());

        // test get dir path
        String expectedDirPath =
                vertexInfo.getPrefix().toJavaString() + propertyGroup.getPrefix().toJavaString();
        Result<StdString> maybeDirPath = vertexInfo.getPathPrefix(propertyGroup);
        Assert.assertFalse(maybeDirPath.hasError());
        Assert.assertEquals(expectedDirPath, maybeDirPath.value().toJavaString());
        // property group not exist
        Assert.assertTrue(vertexInfo.getPathPrefix(propertyGroup2).status().isKeyError());
        // test get file path
        Result<StdString> maybePath = vertexInfo.getFilePath(propertyGroup, 0);
        Assert.assertFalse(maybePath.hasError());
        Assert.assertEquals(expectedDirPath + "chunk0", maybePath.value().toJavaString());
        // property group not exist
        Assert.assertTrue(vertexInfo.getFilePath(propertyGroup2, 0).status().isKeyError());
        // vertex count file path
        Result<StdString> maybePath2 = vertexInfo.getVerticesNumFilePath();
        Assert.assertFalse(maybePath2.hasError());
        Assert.assertEquals(
                vertexInfo.getPrefix().toJavaString() + "vertex_count",
                maybePath2.value().toJavaString());

        // test save
        StdString savePath = StdString.create("/tmp/gar-java-tmp-file");
        Assert.assertTrue(vertexInfo.save(savePath).ok());
        File tempFile = new File(savePath.toJavaString());
        Assert.assertTrue(tempFile.exists());
    }
}
