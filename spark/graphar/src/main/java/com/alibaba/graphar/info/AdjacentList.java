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

import com.alibaba.graphar.info.type.AdjListType;
import com.alibaba.graphar.info.type.FileType;
import com.alibaba.graphar.info.yaml.AdjacentListYamlParser;

public class AdjacentList {
    private final AdjListType type;
    private final FileType fileType;
    private final String prefix;

    public AdjacentList(AdjListType type, FileType fileType, String prefix) {
        this.type = type;
        this.fileType = fileType;
        this.prefix = prefix;
    }

    AdjacentList(AdjacentListYamlParser yamlParser) {
        this.type =
                AdjListType.fromOrderedAndAlignedBy(
                        yamlParser.isOrdered(), yamlParser.isAligned_by());
        this.fileType = FileType.valueOf(yamlParser.getFile_type());
        this.prefix = yamlParser.getPrefix();
    }

    public AdjListType getType() {
        return type;
    }

    public FileType getFileType() {
        return fileType;
    }

    public String getPrefix() {
        return prefix;
    }
}
