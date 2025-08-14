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

package org.apache.graphar.info.yaml;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.graphar.info.PropertyGroup;
import org.apache.graphar.info.type.FileType;

public class PropertyGroupYaml {
    private List<PropertyYaml> properties;
    private String file_type;
    private String prefix;

    public PropertyGroupYaml() {
        this.properties = new ArrayList<>();
        this.file_type = "";
        this.prefix = "";
    }

    public PropertyGroupYaml(PropertyGroup propertyGroup) {
        this.properties =
                propertyGroup.getPropertyList().stream()
                        .map(PropertyYaml::new)
                        .collect(Collectors.toList());
        this.file_type = propertyGroup.getFileType().toString();
        this.prefix = propertyGroup.getPrefix();
    }

    PropertyGroup toPropertyGroup() {
        return new PropertyGroup(
                properties.stream().map(PropertyYaml::toProperty).collect(Collectors.toList()),
                FileType.fromString(file_type),
                prefix);
    }

    public List<PropertyYaml> getProperties() {
        return properties;
    }

    public void setProperties(List<PropertyYaml> properties) {
        this.properties = properties;
    }

    public String getFile_type() {
        return file_type;
    }

    public void setFile_type(String file_type) {
        this.file_type = file_type;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }
}
