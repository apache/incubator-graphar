/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.graphar.datasources.ldbc.stream.model

import scala.collection.mutable

/**
 * Streaming integration statistics
 */
case class IntegrationStatistics(
 var generationDurationMs: Long = 0L,
 var completed: Boolean = false,
 var processedEntities: mutable.Set[String] = mutable.Set[String](),
 var totalRecords: Long = 0L,
 var writerStatistics: Option[Map[String, Long]] = None
)

/**
 * Standard integration statistics (for backward compatibility)
 */
case class standardIntegrationStatistics(
 generationDurationMs: Long = 0L,
 completed: Boolean = false,
 writerStatistics: Option[Any] = None
)