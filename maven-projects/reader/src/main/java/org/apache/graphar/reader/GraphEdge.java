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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** One topology row and its selected GraphAr edge properties. */
public final class GraphEdge {
    private final long source;
    private final long destination;
    private final Map<String, Object> properties;

    GraphEdge(long source, long destination, Map<String, Object> properties) {
        this.source = source;
        this.destination = destination;
        this.properties =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(
                                Objects.requireNonNull(properties, "Properties cannot be null.")));
    }

    /**
     * Returns the source vertex ID.
     *
     * @return the source vertex ID
     */
    public long source() {
        return source;
    }

    /**
     * Returns the destination vertex ID.
     *
     * @return the destination vertex ID
     */
    public long destination() {
        return destination;
    }

    /**
     * Returns the selected property values, including null values.
     *
     * @return the property values by name
     */
    public Map<String, Object> properties() {
        return properties;
    }
}
