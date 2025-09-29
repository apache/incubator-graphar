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

import java.util.Optional;
import org.apache.graphar.info.Property;
import org.apache.graphar.info.type.DataType;

public class PropertyYaml {
    private String name;
    private String data_type;
    private boolean is_primary;
    private Optional<Boolean> is_nullable;
    private String cardinality;

    public PropertyYaml() {
        this.name = "";
        this.data_type = "";
        this.is_primary = false;
        this.is_nullable = Optional.empty();
        this.cardinality = "single"; // Default to single
    }

    public PropertyYaml(Property property) {
        this.name = property.getName();
        this.data_type = property.getDataType().toString();
        this.is_primary = property.isPrimary();
        this.is_nullable = Optional.of(property.isNullable());
    }

    Property toProperty() {
        return new Property(
                name,
                DataType.fromString(data_type),
                is_primary,
                is_nullable.orElseGet(() -> !is_primary));
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getData_type() {
        return data_type;
    }

    public void setData_type(String data_type) {
        this.data_type = data_type;
    }

    public boolean getIs_primary() {
        return is_primary;
    }

    public void setIs_primary(boolean is_primary) {
        this.is_primary = is_primary;
    }

    public boolean getIs_nullable() {
        return is_nullable.orElseGet(() -> !is_primary);
    }

    public void setIs_nullable(boolean is_nullable) {
        this.is_nullable = Optional.of(is_nullable);
    }

    public String getCardinality() {
        return this.cardinality;
    }

    public void setCardinality(String cardinality) {
        this.cardinality = cardinality;
    }
}
