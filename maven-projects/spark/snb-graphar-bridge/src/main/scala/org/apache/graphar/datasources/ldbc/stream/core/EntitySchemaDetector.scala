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

package org.apache.graphar.datasources.ldbc.stream.core

import org.apache.graphar.datasources.ldbc.stream.model.{
  LdbcEntityType,
  VertexEntityType,
  EdgeEntityType,
  PersonVertex,
  ForumVertex,
  PostVertex,
  CommentVertex,
  KnowsEdge,
  HasCreatorEdge,
  ContainerOfEdge,
  ReplyOfEdge,
  LikesEdge,
  HasModeratorEdge,
  HasMemberEdge
}
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

/**
 * Entity Schema Detector
 *
 * Responsible for identifying entity types based on LDBC output header
 * information and establishing field mapping relationships. Supports
 * intelligent recognition based on field characteristics and precise schema
 * matching.
 */
object EntitySchemaDetector {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Entity schema definition
   */
  case class EntitySchema(
      entityType: LdbcEntityType,
      fieldMapping: Map[String, Int], // Field name -> index position
      dataTypes: Map[String, DataType], // Field name -> data type
      isValidSchema: Boolean = true
  ) {
    def getFieldIndex(fieldName: String): Option[Int] =
      fieldMapping.get(fieldName)
    def getDataType(fieldName: String): Option[DataType] =
      dataTypes.get(fieldName)
    def hasField(fieldName: String): Boolean = fieldMapping.contains(fieldName)
  }

  /**
   * Field pattern definition
   */
  private case class FieldPattern(
      requiredFields: Set[String],
      optionalFields: Set[String] = Set.empty,
      fieldCount: Option[Int] = None,
      uniqueIdentifiers: Set[String] = Set.empty
  )

  /**
   * Predefined entity recognition patterns
   */
  private val entityPatterns: Map[LdbcEntityType, FieldPattern] = Map(
    // Vertex entity patterns
    PersonVertex -> FieldPattern(
      requiredFields = Set(
        "id",
        "firstName",
        "lastName",
        "gender",
        "birthday",
        "creationDate"
      ),
      optionalFields =
        Set("locationIP", "browserUsed", "place", "languages", "emails"),
      fieldCount = Some(11),
      uniqueIdentifiers = Set("PersonId", "person.id")
    ),
    ForumVertex -> FieldPattern(
      requiredFields = Set("id", "title", "creationDate"),
      optionalFields = Set("moderatorPersonId"),
      fieldCount = Some(4),
      uniqueIdentifiers = Set("ForumId", "forum.id")
    ),
    PostVertex -> FieldPattern(
      requiredFields = Set(
        "id",
        "imageFile",
        "creationDate",
        "locationIP",
        "browserUsed",
        "language",
        "content"
      ),
      optionalFields =
        Set("creatorPersonId", "creator", "containerForumId", "container"),
      fieldCount = Some(8),
      uniqueIdentifiers = Set("PostId", "post.id", "Message.id")
    ),
    CommentVertex -> FieldPattern(
      requiredFields =
        Set("id", "creationDate", "locationIP", "browserUsed", "content"),
      optionalFields =
        Set("creatorPersonId", "creator", "replyOfPostId", "replyOfCommentId"),
      fieldCount = Some(7),
      uniqueIdentifiers = Set("CommentId", "comment.id", "Message.id")
    ),

    // Edge entity patterns
    KnowsEdge -> FieldPattern(
      requiredFields = Set("Person1Id", "Person2Id", "creationDate"),
      optionalFields = Set("weight"),
      fieldCount = Some(4),
      uniqueIdentifiers = Set("person1", "person2", "knows")
    ),
    HasCreatorEdge -> FieldPattern(
      requiredFields = Set("MessageId", "PersonId"),
      fieldCount = Some(2),
      uniqueIdentifiers = Set("hasCreator", "message", "creator")
    ),
    ContainerOfEdge -> FieldPattern(
      requiredFields = Set("ForumId", "PostId"),
      fieldCount = Some(2),
      uniqueIdentifiers = Set("containerOf", "forum", "post")
    ),
    ReplyOfEdge -> FieldPattern(
      requiredFields = Set("CommentId", "MessageId"),
      fieldCount = Some(2),
      uniqueIdentifiers = Set("replyOf", "comment", "message")
    ),
    LikesEdge -> FieldPattern(
      requiredFields = Set("PersonId", "MessageId", "creationDate"),
      fieldCount = Some(3),
      uniqueIdentifiers = Set("likes", "person", "message")
    ),
    HasModeratorEdge -> FieldPattern(
      requiredFields = Set("ForumId", "PersonId"),
      fieldCount = Some(2),
      uniqueIdentifiers = Set("hasModerator", "forum", "moderator")
    ),
    HasMemberEdge -> FieldPattern(
      requiredFields = Set("ForumId", "PersonId", "joinDate"),
      fieldCount = Some(3),
      uniqueIdentifiers = Set("hasMember", "forum", "member")
    )
  )

  /**
   * Detect entity schema
   */
  def detectSchema(header: Array[String]): EntitySchema = {
    logger.debug(s"Detecting entity schema, header: ${header.mkString(", ")}")

    // Create field mapping
    val fieldMapping = header.zipWithIndex.toMap

    // Try to match various entity patterns
    val detectedEntityType = detectEntityType(header, fieldMapping)

    detectedEntityType match {
      case Some(entityType) =>
        val dataTypes = buildDataTypeMapping(header, entityType)
        val schema = EntitySchema(
          entityType,
          fieldMapping,
          dataTypes,
          isValidSchema = true
        )
        logger.info(s"Successfully detected entity type: ${entityType.name}")
        schema

      case None =>
        logger.warn(
          s"Unable to recognize entity type, header: ${header.mkString(", ")}"
        )
        // Return a default schema, attempt to handle as generic entity
        val dataTypes = buildDefaultDataTypeMapping(header)
        EntitySchema(
          PersonVertex,
          fieldMapping,
          dataTypes,
          isValidSchema = false
        )
    }
  }

  /**
   * Detect entity type
   */
  private def detectEntityType(
      header: Array[String],
      fieldMapping: Map[String, Int]
  ): Option[LdbcEntityType] = {
    val headerFields = header.toSet

    // Try to match various patterns by priority
    val matchScores = entityPatterns.map { case (entityType, pattern) =>
      val score = calculateMatchScore(headerFields, fieldMapping.size, pattern)
      (entityType, score)
    }

    // Find the best match
    val bestMatch = matchScores.maxBy(_._2)
    if (bestMatch._2 > 0.6) { // Match score threshold
      Some(bestMatch._1)
    } else {
      None
    }
  }

  /**
   * Calculate match score
   */
  private def calculateMatchScore(
      headerFields: Set[String],
      fieldCount: Int,
      pattern: FieldPattern
  ): Double = {
    var score = 0.0

    // 1. Required field match score
    val requiredMatches = pattern.requiredFields.count(field =>
      headerFields.exists(_.toLowerCase.contains(field.toLowerCase))
    )
    val requiredScore = if (pattern.requiredFields.nonEmpty) {
      requiredMatches.toDouble / pattern.requiredFields.size
    } else 0.0

    score += requiredScore * 0.6

    // 2. Field count match score
    val fieldCountScore = pattern.fieldCount match {
      case Some(expectedCount) =>
        if (fieldCount == expectedCount) 1.0
        else if (Math.abs(fieldCount - expectedCount) <= 2) 0.5
        else 0.0
      case None => 0.5
    }

    score += fieldCountScore * 0.2

    // 3. Unique identifier match score
    val identifierMatches = pattern.uniqueIdentifiers.count(identifier =>
      headerFields.exists(_.toLowerCase.contains(identifier.toLowerCase))
    )
    val identifierScore = if (pattern.uniqueIdentifiers.nonEmpty) {
      identifierMatches.toDouble / pattern.uniqueIdentifiers.size
    } else 0.0

    score += identifierScore * 0.2

    logger.debug(
      s"Match score calculation: required=$requiredScore, fieldCount=$fieldCountScore, identifier=$identifierScore, total=$score"
    )

    score
  }

  /**
   * Precise field type mapping table based on LDBC raw.scala
   */
  private val LDBC_FIELD_TYPE_MAPPING = Map(
    // Person fields - based on raw.Person
    "id" -> LongType,
    "firstName" -> StringType,
    "lastName" -> StringType,
    "gender" -> StringType,
    "birthday" -> DateType,
    "locationIP" -> StringType,
    "browserUsed" -> StringType,
    "LocationCityId" -> IntegerType,
    "language" -> StringType,
    "email" -> StringType,

    // Forum fields - based on raw.Forum, with camelCase support
    "title" -> StringType, // Explicitly specified as StringType
    "creationDate" -> TimestampType,
    "creationdate" -> TimestampType, // Lowercase version
    "deletionDate" -> TimestampType,
    "deletiondate" -> TimestampType, // Lowercase version
    "explicitlyDeleted" -> BooleanType,
    "ModeratorPersonId" -> LongType,
    "moderatorPersonId" -> LongType, // camelCase version
    "moderatorpersonid" -> LongType, // Lowercase version

    // Post fields - based on raw.Post
    "content" -> StringType, // Explicitly specified as StringType
    "imageFile" -> StringType,
    "length" -> IntegerType,
    "CreatorPersonId" -> LongType,
    "creatorPersonId" -> LongType,
    "ContainerForumId" -> LongType,
    "containerForumId" -> LongType,
    "LocationCountryId" -> LongType,

    // Comment fields - based on raw.Comment
    "ParentPostId" -> LongType,
    "parentPostId" -> LongType,
    "ParentCommentId" -> LongType,
    "parentCommentId" -> LongType,

    // Generic ID fields
    "PersonId" -> LongType,
    "personId" -> LongType,
    "ForumId" -> LongType,
    "forumId" -> LongType,
    "PostId" -> LongType,
    "postId" -> LongType,
    "CommentId" -> LongType,
    "commentId" -> LongType,
    "TagId" -> IntegerType,
    "tagId" -> IntegerType
  )

  /**
   * Infer data type based on field name
   */
  private def inferDataType(fieldName: String): Option[DataType] = {
    val normalizedFieldName = fieldName.toLowerCase.replaceAll("[^a-z0-9]", "")
    logger.info(
      s"Field type inference: original field='$fieldName', normalized field='$normalizedFieldName'"
    )

    // First look for exact mapping
    LDBC_FIELD_TYPE_MAPPING.get(normalizedFieldName) match {
      case Some(dataType) =>
        logger.info(s"Field '$fieldName' using exact type mapping: $dataType")
        Some(dataType)
      case None => {
        logger.info(
          s"Field '$fieldName' exact mapping not found, using fallback inference logic"
        )
        // Fallback inference logic
        val inferredType =
          if (
            normalizedFieldName
              .endsWith("id") || normalizedFieldName.contains("id")
          ) {
            logger.info(
              s"Field '$fieldName' contains 'id', inferred as LongType"
            )
            Some(LongType)
          } else if (
            normalizedFieldName.contains("date") && !normalizedFieldName
              .contains("birthday")
          ) {
            logger.info(
              s"Field '$fieldName' contains 'date' but not 'birthday', inferred as TimestampType"
            )
            Some(TimestampType)
          } else if (
            normalizedFieldName.contains("birthday") || normalizedFieldName
              .contains("birth")
          ) {
            logger.info(
              s"Field '$fieldName' contains 'birthday', inferred as DateType"
            )
            Some(DateType)
          } else if (
            normalizedFieldName.contains("weight") || normalizedFieldName
              .contains("score")
          ) {
            logger.info(
              s"Field '$fieldName' contains 'weight' or 'score', inferred as FloatType"
            )
            Some(FloatType)
          } else if (
            normalizedFieldName.contains("count") || normalizedFieldName
              .contains("size") || normalizedFieldName.contains("length")
          ) {
            logger.info(
              s"Field '$fieldName' contains count-related words, inferred as IntegerType"
            )
            Some(IntegerType)
          } else if (
            normalizedFieldName == "title" || normalizedFieldName.contains(
              "name"
            ) ||
            normalizedFieldName.contains("content") || normalizedFieldName
              .contains("file") ||
            normalizedFieldName
              .contains("ip") || normalizedFieldName.contains("browser") ||
            normalizedFieldName.contains("language") || normalizedFieldName
              .contains("gender")
          ) {
            // Explicitly specify these fields as String type to avoid misclassification
            logger.info(
              s"Field '$fieldName' matches string pattern, inferred as StringType"
            )
            Some(StringType)
          } else {
            logger.info(
              s"Field '$fieldName' unable to match any pattern, default inferred as StringType"
            )
            Some(StringType)
          }

        logger.info(s"Field '$fieldName' final inferred type: $inferredType")
        inferredType
      }
    }
  }

  /**
   * Build data type mapping - based on LDBC unified mapping table
   */
  private def buildDataTypeMapping(
      header: Array[String],
      entityType: LdbcEntityType
  ): Map[String, DataType] = {
    header.map { fieldName =>
      val dataType = detectFieldType(fieldName, "").getOrElse(StringType)
      logger.info(s"Building type mapping: $fieldName -> $dataType")
      fieldName -> dataType
    }.toMap
  }

  /**
   * Build default data type mapping - for unrecognized entity types
   */
  private def buildDefaultDataTypeMapping(
      header: Array[String]
  ): Map[String, DataType] = {
    header.map { fieldName =>
      val dataType = inferDataType(fieldName).getOrElse(StringType)
      fieldName -> dataType
    }.toMap
  }

  /**
   * Validate if record conforms to schema
   */
  def validateRecord(record: Array[String], schema: EntitySchema): Boolean = {
    if (!schema.isValidSchema) {
      return true // For invalid schema, no validation
    }

    // Check field count
    if (record.length != schema.fieldMapping.size) {
      logger.warn(
        s"Record field count mismatch: expected=${schema.fieldMapping.size}, actual=${record.length}"
      )
      return false
    }

    // Can add more data type validation
    true
  }

  /**
   * Get primary key field of entity
   */
  def getPrimaryKeyField(entityType: LdbcEntityType): String = {
    entityType match {
      case _: VertexEntityType  => "id"
      case edge: EdgeEntityType =>
        // For edges, typically the combination of source and destination vertices
        edge match {
          case KnowsEdge        => "Person1Id"
          case HasCreatorEdge   => "MessageId"
          case ContainerOfEdge  => "ForumId"
          case ReplyOfEdge      => "CommentId"
          case LikesEdge        => "PersonId"
          case HasModeratorEdge => "ForumId"
          case HasMemberEdge    => "ForumId"
          case _                => "id"
        }
    }
  }
  def detectFieldType(
      fieldName: String,
      sampleValue: String
  ): Option[DataType] = {
    val normalizedFieldName = fieldName.toLowerCase

    // First check exact mapping
    LDBC_FIELD_TYPE_MAPPING.get(fieldName) match {
      case Some(dataType) => Some(dataType)
      case None           =>
        // Check lowercase mapping
        LDBC_FIELD_TYPE_MAPPING.get(normalizedFieldName) match {
          case Some(dataType) => Some(dataType)
          case None           =>
            // Improved fallback inference logic
            if (fieldName == "title" || fieldName == "content") {
              // Explicitly specify title and content as String type
              Some(StringType)
            } else if (
              normalizedFieldName.endsWith("id") || normalizedFieldName
                .contains("id")
            ) {
              if (normalizedFieldName.contains("tag")) {
                Some(IntegerType) // TagId is typically Integer
              } else {
                Some(LongType)
              }
            } else if (
              normalizedFieldName.contains("date") && !normalizedFieldName
                .contains("birthday")
            ) {
              Some(TimestampType)
            } else if (normalizedFieldName.contains("deleted")) {
              Some(BooleanType)
            } else {
              // Default to String type
              Some(StringType)
            }
        }
    }
  }

  /**
   * Get foreign key fields of entity
   */
  def getForeignKeyFields(entityType: LdbcEntityType): List[String] = {
    entityType match {
      case PostVertex => List("creatorPersonId", "containerForumId")
      case CommentVertex =>
        List("creatorPersonId", "replyOfPostId", "replyOfCommentId")
      case ForumVertex => List("moderatorPersonId")
      case edge: EdgeEntityType =>
        edge match {
          case KnowsEdge        => List("Person1Id", "Person2Id")
          case HasCreatorEdge   => List("MessageId", "PersonId")
          case ContainerOfEdge  => List("ForumId", "PostId")
          case ReplyOfEdge      => List("CommentId", "MessageId")
          case LikesEdge        => List("PersonId", "MessageId")
          case HasModeratorEdge => List("ForumId", "PersonId")
          case HasMemberEdge    => List("ForumId", "PersonId")
          case _                => List.empty
        }
      case _ => List.empty
    }
  }
}
