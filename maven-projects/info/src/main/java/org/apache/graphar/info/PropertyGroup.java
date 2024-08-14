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

package org.apache.graphar.info;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.graphar.proto.DataType;
import org.apache.graphar.proto.FileType;
import org.apache.graphar.util.GeneralParams;

public class PropertyGroup implements Iterable<Property> {
    private final org.apache.graphar.proto.PropertyGroup protoPropertyGroup;
    private final List<Property> cachedPropertyList;
    private final Map<String, Property> cachedPropertyMap;

    public PropertyGroup(List<Property> propertyList, FileType fileType, String prefix) {
        this.cachedPropertyList = List.copyOf(propertyList);
        this.cachedPropertyMap =
                cachedPropertyList.stream()
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Property::getName, Function.identity()));
        this.protoPropertyGroup = org.apache.graphar.proto.PropertyGroup.newBuilder()
                .addAllProperties(cachedPropertyList.stream().map(Property::getProto).collect(Collectors.toList()))
                .setFileType(fileType)
                .setPrefix(prefix)
                .build();
    }

    PropertyGroup(org.apache.graphar.proto.PropertyGroup protoPropertyGroup) {
        this.protoPropertyGroup = protoPropertyGroup;
        this.cachedPropertyList = protoPropertyGroup.getPropertiesList().stream().map(Property::new).collect(Collectors.toUnmodifiableList());
        this.cachedPropertyMap = cachedPropertyList.stream().collect(Collectors.toUnmodifiableMap(Property::getName, Function.identity()));
    }

    public Optional<PropertyGroup> addPropertyAsNew(Property property) {
        if (property == null || cachedPropertyMap.containsKey(property.getName())) {
            return Optional.empty();
        }
        List<Property> newPropertyList =
                Stream.concat(protoPropertyGroup.getPropertiesList().stream().map(Property::new), Stream.of(property))
                        .collect(Collectors.toUnmodifiableList());
        return Optional.of(new PropertyGroup(newPropertyList, protoPropertyGroup.getFileType(), protoPropertyGroup.getPrefix()));
    }

    @Override
    public Iterator<Property> iterator() {
        return cachedPropertyList.iterator();
    }

    @Override
    public String toString() {
        return cachedPropertyList.stream()
                .map(Property::getName)
                .collect(Collectors.joining(GeneralParams.regularSeparator));
    }

    public int size() {
        return cachedPropertyList.size();
    }

    public List<Property> getPropertyList() {
        return cachedPropertyList;
    }

    public Map<String, Property> getCachedPropertyMap() {
        return cachedPropertyMap;
    }

    public FileType getFileType() {
        return protoPropertyGroup.getFileType();
    }

    public String getPrefix() {
        return protoPropertyGroup.getPrefix();
    }

    org.apache.graphar.proto.PropertyGroup getProto() {
        return protoPropertyGroup;
    }
}

/**
 * PropertyGroups is a helper class to manage a list of PropertyGroup objects,
 * which can be used by {@link org.apache.graphar.info.VertexInfo} and {@link org.apache.graphar.info.EdgeInfo} conveniently.
 * but should not be exposed to outer side of the package.
 */
class PropertyGroups {
    private final List<PropertyGroup> propertyGroupList;
    private final Map<String, PropertyGroup> propertyGroupMap;
    private final Map<String, Property> properties;

    PropertyGroups(List<PropertyGroup> propertyGroupList) {
        this.propertyGroupList = List.copyOf(propertyGroupList);
        this.properties =
                propertyGroupList.stream()
                        .flatMap(propertyGroup -> propertyGroup.getCachedPropertyMap().values().stream())
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Property::getName, Function.identity()));
        HashMap<String, PropertyGroup> tempPropertyGroupMap = new HashMap<>(this.properties.size());
        for (PropertyGroup propertyGroup : propertyGroupList) {
            for (Property property : propertyGroup) {
                tempPropertyGroupMap.put(property.getName(), propertyGroup);
            }
        }
        this.propertyGroupMap = Map.copyOf(tempPropertyGroupMap);
    }

    private PropertyGroups(
            List<PropertyGroup> propertyGroupList,
            Map<String, PropertyGroup> propertyGroupMap,
            Map<String, Property> properties) {
        this.propertyGroupList = propertyGroupList;
        this.propertyGroupMap = propertyGroupMap;
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
        List<PropertyGroup> newPropertyGroupsAsList =
                Stream.concat(propertyGroupList.stream(), Stream.of(propertyGroup))
                        .collect(Collectors.toUnmodifiableList());
        Map<String, PropertyGroup> tempPropertyGroupMap = new HashMap<>(propertyGroupMap);
        for (Property property : propertyGroup) {
            tempPropertyGroupMap.put(property.getName(), propertyGroup);
        }
        Map<String, Property> newProperties =
                Stream.concat(
                                properties.values().stream(),
                                propertyGroup.getCachedPropertyMap().values().stream())
                        .collect(
                                Collectors.toUnmodifiableMap(
                                        Property::getName, Function.identity()));
        return Optional.of(
                new PropertyGroups(
                        newPropertyGroupsAsList,
                        Map.copyOf(tempPropertyGroupMap),
                        newProperties));
    }

    boolean hasProperty(String propertyName) {
        return properties.containsKey(propertyName);
    }

    boolean hasPropertyGroup(PropertyGroup propertyGroup) {
        return propertyGroupList.contains(propertyGroup);
    }

    int getPropertyGroupNum() {
        return propertyGroupList.size();
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

    List<PropertyGroup> getPropertyGroupList() {
        return propertyGroupList;
    }

    Map<String, PropertyGroup> getPropertyGroupMap() {
        return propertyGroupMap;
    }

    PropertyGroup getPropertyGroup(String propertyName) {
        checkPropertyExist(propertyName);
        return propertyGroupMap.get(propertyName);
    }

    Map<String, Property> getProperties() {
        return properties;
    }

    private void checkPropertyExist(String propertyName) {
        if (null == propertyName) {
            throw new IllegalArgumentException("Property name is null");
        }
        if (!hasProperty(propertyName)) {
            throw new IllegalArgumentException(
                    "Property " + propertyName + " does not exist in the property group " + this);
        }
    }
}
