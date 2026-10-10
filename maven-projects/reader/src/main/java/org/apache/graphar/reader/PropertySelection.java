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

package org.apache.graphar.reader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.PropertyGroup;

/** The requested properties of one GraphAr property group, in declaration order. */
final class PropertySelection {
    final PropertyGroup group;
    final List<String> names;

    private PropertySelection(PropertyGroup group, List<String> names) {
        this.group = group;
        this.names = List.copyOf(names);
    }

    /**
     * Maps requested property names onto the declared groups that hold them. Groups without a
     * requested property are left out, so their chunks are never opened.
     */
    static List<PropertySelection> select(
            List<PropertyGroup> groups, Collection<String> properties) {
        Objects.requireNonNull(properties, "Selected properties cannot be null.");
        Set<String> requested = new LinkedHashSet<>();
        for (String property : properties) {
            if (property == null || property.isBlank()) {
                throw new IllegalArgumentException("Selected property names cannot be blank.");
            }
            if (!requested.add(property)) {
                throw new IllegalArgumentException("Selected property is duplicated: " + property);
            }
        }
        Set<String> unknown = new LinkedHashSet<>(requested);
        List<PropertySelection> selections = new ArrayList<>();
        for (PropertyGroup group : groups) {
            List<String> names = new ArrayList<>();
            for (Property property : group) {
                if (unknown.remove(property.getName())) {
                    names.add(property.getName());
                }
            }
            if (!names.isEmpty()) {
                selections.add(new PropertySelection(group, names));
            }
        }
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("Undeclared properties: " + unknown);
        }
        return List.copyOf(selections);
    }
}
