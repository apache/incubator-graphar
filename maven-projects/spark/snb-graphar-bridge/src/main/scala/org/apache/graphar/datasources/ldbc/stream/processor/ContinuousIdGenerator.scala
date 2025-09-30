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

package org.apache.graphar.datasources.ldbc.stream.processor

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * 连续ID生成器
 *
 * 负责将LDBC的原始ID转换为GraphAr要求的连续ID。
 * GraphAr要求所有顶点ID必须从0开始连续递增，这对于图算法的性能至关重要。
 *
 * 核心功能：
 * 1. 为每种实体类型维护独立的ID空间
 * 2. 将LDBC原始ID映射为连续的内部ID
 * 3. 支持跨实体的ID引用映射
 * 4. 提供ID映射的验证和统计功能
 */
class ContinuousIdGenerator {

  private val logger: Logger = LoggerFactory.getLogger(classOf[ContinuousIdGenerator])

  // 每种实体类型的ID映射表
  private val personIdMapping = mutable.Map[Long, Long]()
  private val forumIdMapping = mutable.Map[Long, Long]()
  private val postIdMapping = mutable.Map[Long, Long]()
  private val commentIdMapping = mutable.Map[Long, Long]()

  // 静态实体ID映射表
  private val placeIdMapping = mutable.Map[Long, Long]()
  private val tagIdMapping = mutable.Map[Long, Long]()
  private val tagClassIdMapping = mutable.Map[Int, Long]()
  private val organisationIdMapping = mutable.Map[Int, Long]()

  // 每种实体类型的下一个可用ID
  private var nextPersonId = 0L
  private var nextForumId = 0L
  private var nextPostId = 0L
  private var nextCommentId = 0L

  // 静态实体的下一个可用ID
  private var nextPlaceId = 0L
  private var nextTagId = 0L
  private var nextTagClassId = 0L
  private var nextOrganisationId = 0L

  /**
   * 获取或生成指定实体的连续ID
   *
   * @param entityType 实体类型 ("Person", "Forum", "Post", "Comment", "Place", "Tag", "TagClass", "Organisation")
   * @param originalId LDBC原始ID
   * @return 连续的内部ID
   */
  def getNextId(entityType: String, originalId: Long): Long = {
    entityType match {
      case "Person" =>
        personIdMapping.getOrElseUpdate(originalId, {
          val id = nextPersonId
          nextPersonId += 1
          logger.debug(s"Generated Person ID: $originalId -> $id")
          id
        })

      case "Forum" =>
        forumIdMapping.getOrElseUpdate(originalId, {
          val id = nextForumId
          nextForumId += 1
          logger.debug(s"Generated Forum ID: $originalId -> $id")
          id
        })

      case "Post" =>
        postIdMapping.getOrElseUpdate(originalId, {
          val id = nextPostId
          nextPostId += 1
          logger.debug(s"Generated Post ID: $originalId -> $id")
          id
        })

      case "Comment" =>
        commentIdMapping.getOrElseUpdate(originalId, {
          val id = nextCommentId
          nextCommentId += 1
          logger.debug(s"Generated Comment ID: $originalId -> $id")
          id
        })

      case "Place" =>
        placeIdMapping.getOrElseUpdate(originalId, {
          val id = nextPlaceId
          nextPlaceId += 1
          logger.debug(s"Generated Place ID: $originalId -> $id")
          id
        })

      case "Tag" =>
        tagIdMapping.getOrElseUpdate(originalId.toInt, {
          val id = nextTagId
          nextTagId += 1
          logger.debug(s"Generated Tag ID: $originalId -> $id")
          id
        })

      case "TagClass" =>
        tagClassIdMapping.getOrElseUpdate(originalId.toInt, {
          val id = nextTagClassId
          nextTagClassId += 1
          logger.debug(s"Generated TagClass ID: $originalId -> $id")
          id
        })

      case "Organisation" =>
        organisationIdMapping.getOrElseUpdate(originalId.toInt, {
          val id = nextOrganisationId
          nextOrganisationId += 1
          logger.debug(s"Generated Organisation ID: $originalId -> $id")
          id
        })

      case _ =>
        throw new IllegalArgumentException(s"Unsupported entity type: $entityType")
    }
  }

  /**
   * 获取已存在的ID映射（不创建新的）
   */
  def getExistingId(entityType: String, originalId: Long): Option[Long] = {
    entityType match {
      case "Person" => personIdMapping.get(originalId)
      case "Forum" => forumIdMapping.get(originalId)
      case "Post" => postIdMapping.get(originalId)
      case "Comment" => commentIdMapping.get(originalId)
      case "Place" => placeIdMapping.get(originalId)
      case "Tag" => tagIdMapping.get(originalId.toInt)
      case "TagClass" => tagClassIdMapping.get(originalId.toInt)
      case "Organisation" => organisationIdMapping.get(originalId.toInt)
      case _ => None
    }
  }

  /**
   * 批量预注册Person IDs
   * 这个方法用于与现有的Person批处理系统集成
   */
  def preRegisterPersonIds(personIdMap: Map[Long, Long]): Unit = {
    personIdMapping ++= personIdMap
    nextPersonId = if (personIdMap.nonEmpty) personIdMap.values.max + 1 else 0
    logger.info(s"Pre-registered ${personIdMap.size} Person IDs, next available ID: $nextPersonId")
  }

  /**
   * 获取ID映射统计信息
   */
  def getStatistics: IdMappingStatistics = {
    IdMappingStatistics(
      personCount = personIdMapping.size,
      forumCount = forumIdMapping.size,
      postCount = postIdMapping.size,
      commentCount = commentIdMapping.size,
      placeCount = placeIdMapping.size,
      tagCount = tagIdMapping.size,
      tagClassCount = tagClassIdMapping.size,
      organisationCount = organisationIdMapping.size,
      totalMappings = personIdMapping.size + forumIdMapping.size + postIdMapping.size + commentIdMapping.size +
                     placeIdMapping.size + tagIdMapping.size + tagClassIdMapping.size + organisationIdMapping.size
    )
  }

  /**
   * 获取指定实体类型的完整ID映射
   */
  def getIdMapping(entityType: String): Map[Long, Long] = {
    entityType match {
      case "Person" => personIdMapping.toMap
      case "Forum" => forumIdMapping.toMap
      case "Post" => postIdMapping.toMap
      case "Comment" => commentIdMapping.toMap
      case "Place" => placeIdMapping.toMap
      case "Tag" => tagIdMapping.map { case (k, v) => k.toLong -> v }.toMap
      case "TagClass" => tagClassIdMapping.map { case (k, v) => k.toLong -> v }.toMap
      case "Organisation" => organisationIdMapping.map { case (k, v) => k.toLong -> v }.toMap
      case _ => Map.empty
    }
  }

  /**
   * 验证ID映射的连续性
   */
  def validateContinuity(): ValidationResult = {
    val errors = mutable.ListBuffer[String]()

    // 验证每种实体类型的ID连续性
    if (personIdMapping.nonEmpty) {
      val personIds = personIdMapping.values.toSeq.sorted
      if (personIds != (0L until personIds.length)) {
        errors += s"Person IDs are not continuous: expected 0-${personIds.length-1}, got ${personIds.mkString(",")}"
      }
    }

    if (forumIdMapping.nonEmpty) {
      val forumIds = forumIdMapping.values.toSeq.sorted
      if (forumIds != (0L until forumIds.length)) {
        errors += s"Forum IDs are not continuous: expected 0-${forumIds.length-1}, got ${forumIds.mkString(",")}"
      }
    }

    if (postIdMapping.nonEmpty) {
      val postIds = postIdMapping.values.toSeq.sorted
      if (postIds != (0L until postIds.length)) {
        errors += s"Post IDs are not continuous: expected 0-${postIds.length-1}, got ${postIds.mkString(",")}"
      }
    }

    if (commentIdMapping.nonEmpty) {
      val commentIds = commentIdMapping.values.toSeq.sorted
      if (commentIds != (0L until commentIds.length)) {
        errors += s"Comment IDs are not continuous: expected 0-${commentIds.length-1}, got ${commentIds.mkString(",")}"
      }
    }

    if (placeIdMapping.nonEmpty) {
      val placeIds = placeIdMapping.values.toSeq.sorted
      if (placeIds != (0L until placeIds.length)) {
        errors += s"Place IDs are not continuous: expected 0-${placeIds.length-1}, got ${placeIds.mkString(",")}"
      }
    }

    if (tagIdMapping.nonEmpty) {
      val tagIds = tagIdMapping.values.toSeq.sorted
      if (tagIds != (0L until tagIds.length)) {
        errors += s"Tag IDs are not continuous: expected 0-${tagIds.length-1}, got ${tagIds.mkString(",")}"
      }
    }

    if (tagClassIdMapping.nonEmpty) {
      val tagClassIds = tagClassIdMapping.values.toSeq.sorted
      if (tagClassIds != (0L until tagClassIds.length)) {
        errors += s"TagClass IDs are not continuous: expected 0-${tagClassIds.length-1}, got ${tagClassIds.mkString(",")}"
      }
    }

    if (organisationIdMapping.nonEmpty) {
      val organisationIds = organisationIdMapping.values.toSeq.sorted
      if (organisationIds != (0L until organisationIds.length)) {
        errors += s"Organisation IDs are not continuous: expected 0-${organisationIds.length-1}, got ${organisationIds.mkString(",")}"
      }
    }

    if (errors.isEmpty) {
      ValidationResult.success()
    } else {
      ValidationResult.failure(errors.toList)
    }
  }

  /**
   * 清理所有ID映射（仅用于测试）
   */
  def clear(): Unit = {
    personIdMapping.clear()
    forumIdMapping.clear()
    postIdMapping.clear()
    commentIdMapping.clear()
    placeIdMapping.clear()
    tagIdMapping.clear()
    tagClassIdMapping.clear()
    organisationIdMapping.clear()

    nextPersonId = 0L
    nextForumId = 0L
    nextPostId = 0L
    nextCommentId = 0L
    nextPlaceId = 0L
    nextTagId = 0L
    nextTagClassId = 0L
    nextOrganisationId = 0L

    logger.info("All ID mappings cleared")
  }

  /**
   * 输出ID映射摘要信息
   */
  def logSummary(): Unit = {
    val stats = getStatistics
    logger.info("=== ID Mapping Summary ===")
    logger.info(s"Person mappings: ${stats.personCount}")
    logger.info(s"Forum mappings: ${stats.forumCount}")
    logger.info(s"Post mappings: ${stats.postCount}")
    logger.info(s"Comment mappings: ${stats.commentCount}")
    logger.info(s"Place mappings: ${stats.placeCount}")
    logger.info(s"Tag mappings: ${stats.tagCount}")
    logger.info(s"TagClass mappings: ${stats.tagClassCount}")
    logger.info(s"Organisation mappings: ${stats.organisationCount}")
    logger.info(s"Total mappings: ${stats.totalMappings}")
  }
}

/**
 * ID映射统计信息
 */
case class IdMappingStatistics(
  personCount: Int,
  forumCount: Int,
  postCount: Int,
  commentCount: Int,
  placeCount: Int,
  tagCount: Int,
  tagClassCount: Int,
  organisationCount: Int,
  totalMappings: Int
)

/**
 * 验证结果
 */
sealed trait ValidationResult {
  def isSuccess: Boolean
  def errors: List[String]
}

object ValidationResult {
  case object Success extends ValidationResult {
    override def isSuccess: Boolean = true
    override def errors: List[String] = List.empty
  }

  case class Failure(override val errors: List[String]) extends ValidationResult {
    override def isSuccess: Boolean = false
  }

  def success(): ValidationResult = Success
  def failure(errors: List[String]): ValidationResult = Failure(errors)
}