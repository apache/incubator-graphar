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

package org.apache.graphar.datasources.ldbc.model

/**
 * Person intermediate data model for GraphAr conversion
 */
case class PersonData(
  id: Long,                    // GraphAr consecutive ID (generated)
  originalId: Long,            // LDBC original ID for relationship mapping
  firstName: String,
  lastName: String,
  birthday: Long,
  creationDate: Long,
  gender: String,
  browserUsed: String,
  locationIp: String,
  cityId: Int,
  languages: String,           // Semicolon-separated string
  emails: String               // Semicolon-separated string
)

/**
 * Knows relationship data model
 */
case class KnowsData(
  src: Long,                   // Source vertex GraphAr ID
  dst: Long,                   // Destination vertex GraphAr ID
  creationDate: Long,
  weight: Float = 0.0f
)

/**
 * Person has interest Tag relationship data model
 */
case class PersonHasInterestData(
  personId: Long,              // Person GraphAr ID
  tagId: Int,                  // Tag ID (reference only)
  creationDate: Long
)

/**
 * Person work at Organisation relationship data model
 */
case class PersonWorkAtData(
  personId: Long,              // Person GraphAr ID
  organisationId: Long,        // Organisation ID (reference only)
  workFrom: Long,              // Start date
  creationDate: Long
)

/**
 * Person study at University relationship data model
 */
case class PersonStudyAtData(
  personId: Long,              // Person GraphAr ID
  universityId: Long,          // University ID (reference only)
  classYear: Long,             // Class year
  creationDate: Long
)

/**
 * Person is located in Place relationship data model
 */
case class PersonIsLocatedInData(
  personId: Long,              // Person GraphAr ID
  cityId: Int,                 // City ID (reference only)
  creationDate: Long
)

/**
 * Conversion result data model
 */
case class ConversionResult(
  personCount: Long,
  knowsCount: Long,
  interestCount: Long = 0L,
  workAtCount: Long = 0L,
  studyAtCount: Long = 0L,
  locationCount: Long = 0L,
  outputPath: String,
  conversionTime: Long = System.currentTimeMillis(),
  warnings: List[String] = List.empty
)