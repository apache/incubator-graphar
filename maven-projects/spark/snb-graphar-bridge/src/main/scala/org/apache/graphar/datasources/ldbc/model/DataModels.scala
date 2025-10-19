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

package org.apache.graphar.datasources.ldbc.model
/** GraphAr conversion Person intermediate data model */
case class PersonData(
 id: Long, // GraphAr continuous ID (generated)
 originalId: Long, // used for relationship mapping to LDBC original ID
 firstName: String,
 lastName: String,
 birthday: Long,
 creationDate: Long,
 gender: String,
 browserUsed: String,
 locationIp: String,
 cityId: Int,
 languages: String, // semicolon-separated string
 emails: String // semicolon-separated string
)

/** Knows relationship data model */
case class KnowsData(
 src: Long, // SourcevertexGraphAr ID
 dst: Long, // TargetvertexGraphAr ID
 creationDate: Long,
 weight: Float = 0.0f
)

/** Person interest in Tag relationship data model */
case class PersonHasInterestData(
 personId: Long, // Person GraphAr ID
 tagId: Int, // Tag ID (reference only)
 creationDate: Long
)

/** Person works at Organisation relationship data model */
case class PersonWorkAtData(
 personId: Long, // Person GraphAr ID
 organisationId: Long, // Organisation ID (reference only)
 workFrom: Long, // start date
 creationDate: Long
)

/** Person studies at University relationship data model */
case class PersonStudyAtData(
 personId: Long, // Person GraphAr ID
 universityId: Long, // University ID (reference only)
 classYear: Long, // graduation year
 creationDate: Long
)

/** Person located at Place relationship data model */
case class PersonIsLocatedInData(
 personId: Long, // Person GraphAr ID
 cityId: Int, // city ID (reference only)
 creationDate: Long
)

/** Conversion result data model */
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

// ===== Static data models =====

/** Place entity data model */
case class PlaceData(
 id: Long, // GraphAr continuous ID
 originalId: Long, // LDBC original ID
 name: String,
 url: String,
 placeType: String, // type（"City", "Country", "Continent")
 partOfPlaceId: Option[Long] // parent Place ID
)

/** Tag entity data model */
case class TagData(
 id: Long,
 originalId: Long,
 name: String,
 url: String,
 typeTagClassId: Int // belongs to TagClass ID
)

/** TagClass entity data model */
case class TagClassData(
 id: Long,
 originalId: Int,
 name: String,
 url: String,
 subclassOfId: Option[Int] // parent TagClass ID
)

/** Organisation entity data model */
case class OrganisationData(
 id: Long,
 originalId: Int,
 organisationType: String, // "Company" or "University"
 name: String,
 url: String,
 locationId: Int // located at Place ID
)

// ===== Static relationship data models =====

/** Place hierarchical relationship data model */
case class PlaceIsPartOfPlaceData(
 src: Long, // child Place ID
 dst: Long // parent Place ID
)

/** Tag belongs to TagClass relationship data model */
case class TagHasTypeTagClassData(
 src: Long, // Tag ID
 dst: Long // TagClass ID
)

/** TagClass hierarchical relationship data model */
case class TagClassIsSubclassOfTagClassData(
 src: Long, // child TagClass ID
 dst: Long // parent TagClass ID
)

/** Organisation location relationship data model */
case class OrganisationIsLocatedInPlaceData(
 src: Long, // Organisation ID
 dst: Long // Place ID
)