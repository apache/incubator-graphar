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

package org.apache.graphar.info.builder;

import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.graphar.info.*;

public abstract class ElementGenericAbstractBuilder<
        T, B extends ElementGenericAbstractBuilder<T, B>> {

    private final Class<T> elementClass; // Needed for builded class constructor
    private final Class<B> builderClass;

    private List<PropertyGroup> propertyGroupsAsListTemp;

    public VersionInfo version;
    public URI baseUri;
    public PropertyGroups propertyGroups;

    protected ElementGenericAbstractBuilder(Class<T> elementClass, Class<B> builderClass) {
        this.elementClass = elementClass;
        this.builderClass = builderClass;
    }

    private B getSelf() { // Generic safety for children at runtime
        return (B) this;
    }

    public B version(VersionInfo version) {
        this.version = version;

        return getSelf();
    }

    public B version(String version) {
        this.version = VersionParser.getVersion(version);

        return getSelf();
    }

    public B baseUri(URI baseUri) {
        this.baseUri = baseUri;
        return getSelf();
    }

    public B baseUri(String baseUri) {
        this.baseUri = URI.create(baseUri);
        return getSelf();
    }

    public B addPropertyGroup(PropertyGroup propertyGroup) {
        if (propertyGroupsAsListTemp == null) propertyGroupsAsListTemp = new ArrayList<>();
        propertyGroupsAsListTemp.add(propertyGroup);
        return getSelf();
    }

    public B addPropertyGroups(List<PropertyGroup> propertyGroups) {
        if (propertyGroupsAsListTemp == null) propertyGroupsAsListTemp = new ArrayList<>();
        propertyGroupsAsListTemp.addAll(propertyGroups);
        return getSelf();
    }

    public B propertyGroups(PropertyGroups propertyGroups) {
        this.propertyGroups = propertyGroups;
        return getSelf();
    }

    protected abstract void check();

    public final T build() {
        check();

        if (propertyGroups == null && propertyGroupsAsListTemp != null) {
            propertyGroups = new PropertyGroups(propertyGroupsAsListTemp);
        } else if (propertyGroupsAsListTemp != null) {
            propertyGroups =
                    propertyGroupsAsListTemp.stream()
                            .map(propertyGroups::addPropertyGroupAsNew)
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .reduce((first, second) -> second)
                            .orElse(new PropertyGroups(new ArrayList<>()));
        }

        if (propertyGroups == null) {
            throw new IllegalArgumentException("PropertyGroups is empty");
        }

        try {
            Constructor<T> elementBuilderConstructor = elementClass.getConstructor(builderClass);
            return elementBuilderConstructor.newInstance(getSelf());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
