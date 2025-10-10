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
/** GraphAr转换用的Person中间数据模型 */
case class PersonData(
                       id: Long,                    // GraphAr连续ID（生成的）
                       originalId: Long,            // 用于关系映射的LDBC原始ID
                       firstName: String,
                       lastName: String,
                       birthday: Long,
                       creationDate: Long,
                       gender: String,
                       browserUsed: String,
                       locationIp: String,
                       cityId: Int,
                       languages: String,           // 分号分隔的字符串
                       emails: String               // 分号分隔的字符串
                     )

/** Knows关系数据模型 */
case class KnowsData(
                      src: Long,                   // 源顶点GraphAr ID
                      dst: Long,                   // 目标顶点GraphAr ID
                      creationDate: Long,
                      weight: Float = 0.0f
                    )

/** Person对Tag的兴趣关系数据模型 */
case class PersonHasInterestData(
                                  personId: Long,              // Person的GraphAr ID
                                  tagId: Int,                  // Tag ID（仅引用）
                                  creationDate: Long
                                )

/** Person在Organisation工作的关系数据模型 */
case class PersonWorkAtData(
                             personId: Long,              // Person的GraphAr ID
                             organisationId: Long,        // Organisation ID（仅引用）
                             workFrom: Long,              // 开始日期
                             creationDate: Long
                           )

/** Person在University学习的关系数据模型 */
case class PersonStudyAtData(
                              personId: Long,              // Person的GraphAr ID
                              universityId: Long,          // University ID（仅引用）
                              classYear: Long,             // 毕业年份
                              creationDate: Long
                            )

/** Person位于Place的关系数据模型 */
case class PersonIsLocatedInData(
                                  personId: Long,              // Person的GraphAr ID
                                  cityId: Int,                 // 城市ID（仅引用）
                                  creationDate: Long
                                )

/** 转换结果数据模型 */
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

// ===== 静态实体数据模型 =====

/** Place实体数据模型 */
case class PlaceData(
                      id: Long,                    // GraphAr连续ID
                      originalId: Long,            // LDBC原始ID
                      name: String,
                      url: String,
                      placeType: String,           // 类型（"City", "Country", "Continent"）
                      partOfPlaceId: Option[Long]  // 父级Place ID
                    )

/** Tag实体数据模型 */
case class TagData(
                    id: Long,
                    originalId: Long,
                    name: String,
                    url: String,
                    typeTagClassId: Int          // 所属TagClass ID
                  )

/** TagClass实体数据模型 */
case class TagClassData(
                         id: Long,
                         originalId: Int,
                         name: String,
                         url: String,
                         subclassOfId: Option[Int]    // 父级TagClass ID
                       )

/** Organisation实体数据模型 */
case class OrganisationData(
                             id: Long,
                             originalId: Int,
                             organisationType: String,    // "Company" 或 "University"
                             name: String,
                             url: String,
                             locationId: Int              // 所在Place ID
                           )

// ===== 静态关系数据模型 =====

/** Place层次关系数据模型 */
case class PlaceIsPartOfPlaceData(
                                   src: Long,                   // 子级Place ID
                                   dst: Long                    // 父级Place ID
                                 )

/** Tag分类关系数据模型 */
case class TagHasTypeTagClassData(
                                   src: Long,                   // Tag ID
                                   dst: Long                    // TagClass ID
                                 )

/** TagClass层次关系数据模型 */
case class TagClassIsSubclassOfTagClassData(
                                             src: Long,                   // 子级TagClass ID
                                             dst: Long                    // 父级TagClass ID
                                           )

/** Organisation位置关系数据模型 */
case class OrganisationIsLocatedInPlaceData(
                                             src: Long,                   // Organisation ID
                                             dst: Long                    // Place ID
                                           )