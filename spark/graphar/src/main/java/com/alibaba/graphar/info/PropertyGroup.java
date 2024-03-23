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

import com.alibaba.graphar.info.type.DataType;
import com.alibaba.graphar.info.type.FileType;
import com.alibaba.graphar.info.yaml.PropertyGroupYamlParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;

public class PropertyGroup implements Iterable<Property> {
    private final ImmutableMap<String, Property> properties;
    private final FileType fileType;
    private final String prefix;

    public PropertyGroup(Map<String, Property> properties, FileType fileType, String prefix) {
        this.properties =
                properties instanceof ImmutableMap
                        ? (ImmutableMap<String, Property>) properties
                        : ImmutableMap.copyOf(properties);
        this.fileType = fileType;
        this.prefix = prefix;
    }

    public PropertyGroup(List<Property> properties, FileType fileType, String prefix) {
        this(
                properties.stream()
                        .collect(
                                ImmutableMap.toImmutableMap(
                                        Property::getName, Function.identity())),
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

    public int size() {
        return properties.size();
    }

    public ImmutableMap<String, Property> getProperties() {
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
    private final ImmutableList<PropertyGroup> propertyGroupsAsList;
    private final ImmutableMap<String, PropertyGroup> propertyGroupsAsMap;
    private final ImmutableMap<String, Property> properties;

    PropertyGroups(List<PropertyGroup> propertyGroupsAsList) {
        this.propertyGroupsAsList =
                propertyGroupsAsList instanceof ImmutableList
                        ? (ImmutableList<PropertyGroup>) propertyGroupsAsList
                        : ImmutableList.copyOf(propertyGroupsAsList);
        this.properties =
                propertyGroupsAsList.stream()
                        .flatMap(propertyGroup -> propertyGroup.getProperties().values().stream())
                        .collect(
                                ImmutableMap.toImmutableMap(
                                        Property::getName, Function.identity()));
        this.propertyGroupsAsMap =
                propertyGroupsAsList.stream()
                        .collect(
                                ImmutableMap.toImmutableMap(
                                        PropertyGroup::getPrefix, Function.identity()));
    }

    private PropertyGroups(
            ImmutableList<PropertyGroup> propertyGroupsAsList,
            ImmutableMap<String, PropertyGroup> propertyGroupsAsMap,
            ImmutableMap<String, Property> properties) {
        this.propertyGroupsAsList = propertyGroupsAsList;
        this.propertyGroupsAsMap = propertyGroupsAsMap;
        this.properties = properties;
    }

    Optional<PropertyGroups> addPropertyGroupAsNew(PropertyGroup propertyGroup) {
        if (propertyGroup == null || propertyGroup.size() == 0 || hasPropertyGroup(propertyGroup)) {
            return Optional.empty();
        }
        for (Property property : propertyGroup) {
            if (hasProperty(property.getName())) {
                return Optional.empty();
            }
        }
        ImmutableList<PropertyGroup> newPropertyGroupsAsList =
                Stream.concat(propertyGroupsAsList.stream(), Stream.of(propertyGroup))
                        .collect(ImmutableList.toImmutableList());
        ImmutableMap.Builder<String, PropertyGroup> pgsMapBuilder = ImmutableMap.<String, PropertyGroup>builder().putAll(propertyGroupsAsMap);
        for (Property property : propertyGroup) {
            pgsMapBuilder.put(property.getName(), propertyGroup);
        }
        ImmutableMap<String, Property> newProperties = ImmutableMap.<String, Property>builder().putAll(properties).putAll(propertyGroup.getProperties()).build();
        return Optional.of(
                new PropertyGroups(
                        newPropertyGroupsAsList,
                        pgsMapBuilder.build(),
                        newProperties));
    }

    boolean hasProperty(String propertyName) {
        return properties.containsKey(propertyName);
    }

    boolean hasPropertyGroup(PropertyGroup propertyGroup) {
        return propertyGroupsAsList.contains(propertyGroup);
    }

    int getPropertyGroupNum() {
        return propertyGroupsAsMap.values().size();
    }

    DataType getPropertyType(String propertyName) {
        checkPropertyExist(propertyName);
        return properties.get(propertyName).getDataType();
    }

    boolean isPrimaryKey(String propertyName) {
        checkPropertyExist(propertyName);
        return properties.get(propertyName).isPrimary();
    }

    boolean isNullableKey(String propertyName) {
        checkPropertyExist(propertyName);
        return properties.get(propertyName).isNullable();
    }

    ImmutableList<PropertyGroup> toList() {
        return propertyGroupsAsList;
    }

    PropertyGroup getPropertyGroup(String propertyName) {
        checkPropertyExist(propertyName);
        return propertyGroupsAsMap.get(propertyName);
    }

    ImmutableMap<String, Property> getProperties() {
        return properties;
    }

    Property getProperty(String propertyName) {
        checkPropertyExist(propertyName);
        return properties.get(propertyName);
    }

    private void checkPropertyExist(String propertyName) {
        if (!hasProperty(propertyName)) {
            throw new IllegalArgumentException(
                    // TODO: To specify the property group name, find out how to convert the
                    // property group to a string.
                    // In another words, found out do sequence of properties in the property group
                    // matter?
                    "Property " + propertyName + " does not exist in the property groups");
        }
    }
}
