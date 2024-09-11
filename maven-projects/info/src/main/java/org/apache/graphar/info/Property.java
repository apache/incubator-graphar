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

import org.apache.graphar.proto.DataType;
import org.apache.graphar.info.yaml.PropertyYaml;

public class Property {
    private final org.apache.graphar.proto.Property protoProperty;

    public Property(String name, DataType dataType, boolean primary, boolean nullable) {
        protoProperty =
                org.apache.graphar.proto.Property.newBuilder()
                        .setName(name)
                        .setType(dataType)
                        .setIsPrimaryKey(primary)
                        .setIsNullable(nullable)
                        .build();
    }

    private Property(org.apache.graphar.proto.Property protoProperty) {
        this.protoProperty = protoProperty;
    }

    public static Property ofProto(org.apache.graphar.proto.Property protoProperty) {
        return new Property(protoProperty);
    }

    public String getName() {
        return protoProperty.getName();
    }

    public DataType getDataType() {
        return protoProperty.getType();
    }

    public boolean isPrimary() {
        return protoProperty.getIsPrimaryKey();
    }

    public boolean isNullable() {
        return protoProperty.getIsNullable();
    }

    org.apache.graphar.proto.Property getProto() {
        return protoProperty;
    }
}
