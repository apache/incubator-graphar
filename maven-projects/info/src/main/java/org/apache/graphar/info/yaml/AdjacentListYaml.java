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

public class AdjacentListYaml {
    private boolean ordered;
    private String aligned_by;
    private String file_type;
    private String prefix;

    public AdjacentListYaml() {
        this.ordered = false;
        this.aligned_by = "";
        this.file_type = "";
        this.prefix = "";
    }

    public AdjacentListYaml(org.apache.graphar.info.AdjacentList adjacentList) {
        this.ordered = adjacentList.getType().isOrdered();
        this.aligned_by = adjacentList.getType().getAlignedBy();
        this.file_type = adjacentList.getFileType().toString();
        this.prefix = adjacentList.getPrefix();
    }

    public boolean isOrdered() {
        return ordered;
    }

    public void setOrdered(boolean ordered) {
        this.ordered = ordered;
    }

    public String isAligned_by() {
        return aligned_by;
    }

    public void setAligned_by(String aligned_by) {
        this.aligned_by = aligned_by;
    }

    public String getFile_type() {
        return file_type;
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
