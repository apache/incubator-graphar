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

package com.alibaba.graphar.info;

import com.alibaba.graphar.info.type.FileType;
import com.alibaba.graphar.info.yaml.PropertyGroupYamlParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

public class PropertyGroup implements Iterable<Property> {
    private final Map<String, Property> properties;
    private final FileType fileType;
    private final String prefix;

    public PropertyGroup(Map<String, Property> properties, FileType fileType, String prefix) {
        this.properties = properties;
        this.fileType = fileType;
        this.prefix = prefix;
    }

    public PropertyGroup(List<Property> properties, FileType fileType, String prefix) {
        this(
                properties.stream()
                        .collect(Collectors.toMap(Property::getName, Function.identity())),
                fileType,
                prefix);
    }

    PropertyGroup(PropertyGroupYamlParser yamlParser) {
        this(
                yamlParser.getProperties().stream().map(Property::new).collect(Collectors.toList()),
                yamlParser.getFile_type(),
                yamlParser.getPrefix());
    }

    @NotNull
    @Override
    public Iterator<Property> iterator() {
        return properties.values().iterator();
    }

    public Map<String, Property> getProperties() {
        return properties;
    }

    public FileType getFileType() {
        return fileType;
    }

    public String getPrefix() {
        return prefix;
    }
}

class PropertyGroups {
    private final List<PropertyGroup> propertyGroupsAsList;
    private final Map<String, PropertyGroup> propertyGroupsAsMap;
    private final Map<String, Property> properties;

    PropertyGroups(List<PropertyGroup> propertyGroupsAsList) {
        this.propertyGroupsAsList = propertyGroupsAsList;
        this.propertyGroupsAsMap =
                propertyGroupsAsList.stream()
                        .collect(Collectors.toMap(PropertyGroup::getPrefix, Function.identity()));
        this.properties =
                propertyGroupsAsList.stream()
                        .flatMap(propertyGroup -> propertyGroup.getProperties().values().stream())
                        .collect(Collectors.toMap(Property::getName, Function.identity()));
    }

    private PropertyGroups(
            List<PropertyGroup> propertyGroupsAsList,
            Map<String, PropertyGroup> propertyGroupsAsMap,
            Map<String, Property> properties) {
        this.propertyGroupsAsList = propertyGroupsAsList;
        this.propertyGroupsAsMap = propertyGroupsAsMap;
        this.properties = properties;
    }

    PropertyGroups addPropertyGroup(PropertyGroup propertyGroup) {
        List<PropertyGroup> newPropertyGroups = new ArrayList<>(propertyGroupsAsList);
        newPropertyGroups.add(propertyGroup);
        Map<String, PropertyGroup> newPropertyGroupsAsMap = new HashMap<>(propertyGroupsAsMap);
        newPropertyGroupsAsMap.put(propertyGroup.getPrefix(), propertyGroup);
        Map<String, Property> newProperties = new HashMap<>(properties);
        newProperties.putAll(propertyGroup.getProperties());
        return new PropertyGroups(newPropertyGroups, newPropertyGroupsAsMap, newProperties);
    }

    public boolean hasProperty(String propertyName) {
        return properties.containsKey(propertyName);
    }

    public boolean hasPropertyGroup(PropertyGroup propertyGroup) {
        return propertyGroupsAsList.contains(propertyGroup);
    }

    public int getPropertyGroupNum() {
        return propertyGroupsAsMap.values().size();
    }

    boolean isPrimaryKey(String propertyName) {
        return properties.get(propertyName).isPrimary();
    }

    boolean isNullableKey(String propertyName) {
        return properties.get(propertyName).isNullable();
    }

    List<PropertyGroup> toList() {
        return propertyGroupsAsList;
    }

    PropertyGroup getPropertyGroup(String propertyName) {
        return propertyGroupsAsMap.get(propertyName);
    }

    Map<String, Property> getProperties() {
        return properties;
    }

    Property getProperty(String propertyName) {
        return properties.get(propertyName);
    }
}
