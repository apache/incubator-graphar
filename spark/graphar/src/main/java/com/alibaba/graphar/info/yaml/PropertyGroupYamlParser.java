/*
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.info.yaml;

import com.alibaba.graphar.info.type.FileType;
import java.util.ArrayList;
import java.util.List;

public class PropertyGroupYamlParser {
    private List<PropertyYamlParser> properties;
    private String file_type;
    private String prefix;

    public PropertyGroupYamlParser() {
        this.properties = new ArrayList<>();
        this.file_type = "";
        this.prefix = "";
    }

    public List<PropertyYamlParser> getProperties() {
        return properties;
    }

    public void setProperties(List<PropertyYamlParser> properties) {
        this.properties = properties;
    }

    public FileType getFile_type() {
        return FileType.valueOf(file_type);
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
