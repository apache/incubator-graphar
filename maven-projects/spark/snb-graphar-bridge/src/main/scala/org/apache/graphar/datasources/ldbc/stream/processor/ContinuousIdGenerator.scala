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
 * Continuous ID generator
 *
 * Responsible for converting LDBC original IDs to GraphAr required continuous
 * IDs. GraphAr requires all vertex IDs to be continuously incrementing from 0,
 * which is critical for graph algorithm performance.
 *
 * Core features:
 *   1. Maintain independent ID space for each entity type 2. Map LDBC original
 *      IDs to continuous internal IDs 3. Support cross-entity ID reference
 *      mapping 4. Provide ID mapping verification and statistics features
 */
class ContinuousIdGenerator {

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[ContinuousIdGenerator])

  // ID mapping table for each entity type
  private val personIdmapping = mutable.Map[Long, Long]()
  private val forumIdmapping = mutable.Map[Long, Long]()
  private val postIdmapping = mutable.Map[Long, Long]()
  private val commentIdmapping = mutable.Map[Long, Long]()

  // Static entity ID mapping table
  private val placeIdmapping = mutable.Map[Long, Long]()
  private val tagIdmapping = mutable.Map[Long, Long]()
  private val tagClassIdmapping = mutable.Map[Int, Long]()
  private val organisationIdmapping = mutable.Map[Int, Long]()

  // Next available ID for each entity type
  private var nextPersonId = 0L
  private var nextForumId = 0L
  private var nextPostId = 0L
  private var nextCommentId = 0L

  // Next available ID for static entities
  private var nextPlaceId = 0L
  private var nextTagId = 0L
  private var nextTagClassId = 0L
  private var nextOrganisationId = 0L

  /**
   * Get or generate continuous ID for specified entity
   *
   * @param entityType
   *   EntityType ("Person", "Forum", "Post", "Comment", "Place", "Tag",
   *   "TagClass", "Organisation")
   * @param originalId
   *   LDBC original ID
   * @return
   *   Continuous internal ID
   */
  def getNextId(entityType: String, originalId: Long): Long = {
    entityType match {
      case "Person" =>
        personIdmapping.getOrElseUpdate(
          originalId, {
            val id = nextPersonId
            nextPersonId += 1
            logger.debug(s"Generated Person ID: $originalId -> $id")
            id
          }
        )

      case "Forum" =>
        forumIdmapping.getOrElseUpdate(
          originalId, {
            val id = nextForumId
            nextForumId += 1
            logger.debug(s"Generated Forum ID: $originalId -> $id")
            id
          }
        )

      case "Post" =>
        postIdmapping.getOrElseUpdate(
          originalId, {
            val id = nextPostId
            nextPostId += 1
            logger.debug(s"Generated Post ID: $originalId -> $id")
            id
          }
        )

      case "Comment" =>
        commentIdmapping.getOrElseUpdate(
          originalId, {
            val id = nextCommentId
            nextCommentId += 1
            logger.debug(s"Generated Comment ID: $originalId -> $id")
            id
          }
        )

      case "Place" =>
        placeIdmapping.getOrElseUpdate(
          originalId, {
            val id = nextPlaceId
            nextPlaceId += 1
            logger.debug(s"Generated Place ID: $originalId -> $id")
            id
          }
        )

      case "Tag" =>
        tagIdmapping.getOrElseUpdate(
          originalId.toInt, {
            val id = nextTagId
            nextTagId += 1
            logger.debug(s"Generated Tag ID: $originalId -> $id")
            id
          }
        )

      case "TagClass" =>
        tagClassIdmapping.getOrElseUpdate(
          originalId.toInt, {
            val id = nextTagClassId
            nextTagClassId += 1
            logger.debug(s"Generated TagClass ID: $originalId -> $id")
            id
          }
        )

      case "Organisation" =>
        organisationIdmapping.getOrElseUpdate(
          originalId.toInt, {
            val id = nextOrganisationId
            nextOrganisationId += 1
            logger.debug(s"Generated Organisation ID: $originalId -> $id")
            id
          }
        )

      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported entity type: $entityType"
        )
    }
  }

  /**
   * Get existing ID mapping (do not create new)
   */
  def getExistingId(entityType: String, originalId: Long): Option[Long] = {
    entityType match {
      case "Person"       => personIdmapping.get(originalId)
      case "Forum"        => forumIdmapping.get(originalId)
      case "Post"         => postIdmapping.get(originalId)
      case "Comment"      => commentIdmapping.get(originalId)
      case "Place"        => placeIdmapping.get(originalId)
      case "Tag"          => tagIdmapping.get(originalId.toInt)
      case "TagClass"     => tagClassIdmapping.get(originalId.toInt)
      case "Organisation" => organisationIdmapping.get(originalId.toInt)
      case _              => None
    }
  }

  /**
   * Batch pre-register Person IDs This method is used to integrate with
   * existing Person batch processing system
   */
  def preRegisterPersonIds(personIdMap: Map[Long, Long]): Unit = {
    personIdmapping ++= personIdMap
    nextPersonId = if (personIdMap.nonEmpty) personIdMap.values.max + 1 else 0
    logger.info(
      s"Pre-registered ${personIdMap.size} Person IDs, next available ID: $nextPersonId"
    )
  }

  /**
   * GetIDmappingStatistics
   */
  def getStatistics: IdmappingStatistics = {
    IdmappingStatistics(
      personCount = personIdmapping.size,
      forumCount = forumIdmapping.size,
      postCount = postIdmapping.size,
      commentCount = commentIdmapping.size,
      placeCount = placeIdmapping.size,
      tagCount = tagIdmapping.size,
      tagClassCount = tagClassIdmapping.size,
      organisationCount = organisationIdmapping.size,
      totalmappings =
        personIdmapping.size + forumIdmapping.size + postIdmapping.size + commentIdmapping.size +
          placeIdmapping.size + tagIdmapping.size + tagClassIdmapping.size + organisationIdmapping.size
    )
  }

  /**
   * Get complete ID mapping for specified entity type
   */
  def getIdmapping(entityType: String): Map[Long, Long] = {
    entityType match {
      case "Person"  => personIdmapping.toMap
      case "Forum"   => forumIdmapping.toMap
      case "Post"    => postIdmapping.toMap
      case "Comment" => commentIdmapping.toMap
      case "Place"   => placeIdmapping.toMap
      case "Tag"     => tagIdmapping.map { case (k, v) => k.toLong -> v }.toMap
      case "TagClass" =>
        tagClassIdmapping.map { case (k, v) => k.toLong -> v }.toMap
      case "Organisation" =>
        organisationIdmapping.map { case (k, v) => k.toLong -> v }.toMap
      case _ => Map.empty
    }
  }

  /**
   * Verify ID mapping continuity
   */
  def validateContinuity(): ValidationResult = {
    val errors = mutable.ListBuffer[String]()

    // Verify ID continuity for each entity type
    if (personIdmapping.nonEmpty) {
      val personIds = personIdmapping.values.toSeq.sorted
      if (personIds != (0L until personIds.length)) {
        errors += s"Person IDs are not continuous: expected 0-${personIds.length - 1}, got ${personIds
          .mkString(",")}"
      }
    }

    if (forumIdmapping.nonEmpty) {
      val forumIds = forumIdmapping.values.toSeq.sorted
      if (forumIds != (0L until forumIds.length)) {
        errors += s"Forum IDs are not continuous: expected 0-${forumIds.length - 1}, got ${forumIds.mkString(",")}"
      }
    }

    if (postIdmapping.nonEmpty) {
      val postIds = postIdmapping.values.toSeq.sorted
      if (postIds != (0L until postIds.length)) {
        errors += s"Post IDs are not continuous: expected 0-${postIds.length - 1}, got ${postIds.mkString(",")}"
      }
    }

    if (commentIdmapping.nonEmpty) {
      val commentIds = commentIdmapping.values.toSeq.sorted
      if (commentIds != (0L until commentIds.length)) {
        errors += s"Comment IDs are not continuous: expected 0-${commentIds.length - 1}, got ${commentIds
          .mkString(",")}"
      }
    }

    if (placeIdmapping.nonEmpty) {
      val placeIds = placeIdmapping.values.toSeq.sorted
      if (placeIds != (0L until placeIds.length)) {
        errors += s"Place IDs are not continuous: expected 0-${placeIds.length - 1}, got ${placeIds.mkString(",")}"
      }
    }

    if (tagIdmapping.nonEmpty) {
      val tagIds = tagIdmapping.values.toSeq.sorted
      if (tagIds != (0L until tagIds.length)) {
        errors += s"Tag IDs are not continuous: expected 0-${tagIds.length - 1}, got ${tagIds.mkString(",")}"
      }
    }

    if (tagClassIdmapping.nonEmpty) {
      val tagClassIds = tagClassIdmapping.values.toSeq.sorted
      if (tagClassIds != (0L until tagClassIds.length)) {
        errors += s"TagClass IDs are not continuous: expected 0-${tagClassIds.length - 1}, got ${tagClassIds
          .mkString(",")}"
      }
    }

    if (organisationIdmapping.nonEmpty) {
      val organisationIds = organisationIdmapping.values.toSeq.sorted
      if (organisationIds != (0L until organisationIds.length)) {
        errors += s"Organisation IDs are not continuous: expected 0-${organisationIds.length - 1}, got ${organisationIds
          .mkString(",")}"
      }
    }

    if (errors.isEmpty) {
      ValidationResult.success()
    } else {
      ValidationResult.failure(errors.toList)
    }
  }

  /**
   * Clean up all ID mappings (only used for testing)
   */
  def clear(): Unit = {
    personIdmapping.clear()
    forumIdmapping.clear()
    postIdmapping.clear()
    commentIdmapping.clear()
    placeIdmapping.clear()
    tagIdmapping.clear()
    tagClassIdmapping.clear()
    organisationIdmapping.clear()

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
   * outputIDmappingsummaryinformation
   */
  def logSummary(): Unit = {
    val stats = getStatistics
    logger.info("=== ID mapping Summary ===")
    logger.info(s"Person mappings: ${stats.personCount}")
    logger.info(s"Forum mappings: ${stats.forumCount}")
    logger.info(s"Post mappings: ${stats.postCount}")
    logger.info(s"Comment mappings: ${stats.commentCount}")
    logger.info(s"Place mappings: ${stats.placeCount}")
    logger.info(s"Tag mappings: ${stats.tagCount}")
    logger.info(s"TagClass mappings: ${stats.tagClassCount}")
    logger.info(s"Organisation mappings: ${stats.organisationCount}")
    logger.info(s"Total mappings: ${stats.totalmappings}")
  }
}

/**
 * IDmappingStatistics
 */
case class IdmappingStatistics(
    personCount: Int,
    forumCount: Int,
    postCount: Int,
    commentCount: Int,
    placeCount: Int,
    tagCount: Int,
    tagClassCount: Int,
    organisationCount: Int,
    totalmappings: Int
)

/**
 * VerifyResult
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

  case class Failure(override val errors: List[String])
      extends ValidationResult {
    override def isSuccess: Boolean = false
  }

  def success(): ValidationResult = Success
  def failure(errors: List[String]): ValidationResult = Failure(errors)
}
