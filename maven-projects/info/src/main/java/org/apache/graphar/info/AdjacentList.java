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

import org.apache.graphar.proto.AdjListType;
import org.apache.graphar.proto.FileType;

public class AdjacentList {
    private final org.apache.graphar.proto.AdjacentList protoAdjacentList;

    public AdjacentList(AdjListType type, FileType fileType, String prefix) {
        protoAdjacentList =
                org.apache.graphar.proto.AdjacentList.newBuilder()
                        .setType(type)
                        .setFileType(fileType)
                        .setPrefix(prefix)
                        .build();
    }

    public AdjListType getType() {
        return protoAdjacentList.getType();
    }

    public FileType getFileType() {
        return protoAdjacentList.getFileType();
    }

    public String getPrefix() {
        return protoAdjacentList.getPrefix();
    }

    org.apache.graphar.proto.AdjacentList getProto() {
        return protoAdjacentList;
    }
}
