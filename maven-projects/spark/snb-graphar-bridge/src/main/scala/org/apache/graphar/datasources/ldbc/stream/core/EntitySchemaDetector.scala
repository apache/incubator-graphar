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

import org.apache.graphar.datasources.ldbc.stream.model.{LdbcEntityType, VertexEntityType, EdgeEntityType, PersonVertex, ForumVertex, PostVertex, CommentVertex, KnowsEdge, HasCreatorEdge, ContainerOfEdge, ReplyOfEdge, LikesEdge, HasModeratorEdge, HasMemberEdge}
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

/**
 * 实体模式检测器
 *
 * 负责根据LDBC输出的header信息识别实体类型，并建立字段映射关系。
 * 支持基于字段特征的智能识别和精确的模式匹配。
 */
object EntitySchemaDetector {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 实体模式定义
   */
  case class EntitySchema(
    entityType: LdbcEntityType,
    fieldMapping: Map[String, Int], // 字段名 -> 索引位置
    dataTypes: Map[String, DataType], // 字段名 -> 数据类型
    isValidSchema: Boolean = true
  ) {
    def getFieldIndex(fieldName: String): Option[Int] = fieldMapping.get(fieldName)
    def getDataType(fieldName: String): Option[DataType] = dataTypes.get(fieldName)
    def hasField(fieldName: String): Boolean = fieldMapping.contains(fieldName)
  }

  /**
   * 字段特征模式定义
   */
  private case class FieldPattern(
    requiredFields: Set[String],
    optionalFields: Set[String] = Set.empty,
    fieldCount: Option[Int] = None,
    uniqueIdentifiers: Set[String] = Set.empty
  )

  /**
   * 预定义的实体识别模式
   */
  private val entityPatterns: Map[LdbcEntityType, FieldPattern] = Map(
    // 顶点实体模式
    PersonVertex -> FieldPattern(
      requiredFields = Set("id", "firstName", "lastName", "gender", "birthday", "creationDate"),
      optionalFields = Set("locationIP", "browserUsed", "place", "languages", "emails"),
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
      requiredFields = Set("id", "imageFile", "creationDate", "locationIP", "browserUsed", "language", "content"),
      optionalFields = Set("creatorPersonId", "creator", "containerForumId", "container"),
      fieldCount = Some(8),
      uniqueIdentifiers = Set("PostId", "post.id", "Message.id")
    ),

    CommentVertex -> FieldPattern(
      requiredFields = Set("id", "creationDate", "locationIP", "browserUsed", "content"),
      optionalFields = Set("creatorPersonId", "creator", "replyOfPostId", "replyOfCommentId"),
      fieldCount = Some(7),
      uniqueIdentifiers = Set("CommentId", "comment.id", "Message.id")
    ),

    // 边实体模式
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
   * 检测实体模式
   */
  def detectSchema(header: Array[String]): EntitySchema = {
    logger.debug(s"检测实体模式，header: ${header.mkString(", ")}")

    // 创建字段映射
    val fieldMapping = header.zipWithIndex.toMap

    // 尝试匹配各种实体模式
    val detectedEntityType = detectEntityType(header, fieldMapping)

    detectedEntityType match {
      case Some(entityType) =>
        val dataTypes = buildDataTypeMapping(header, entityType)
        val schema = EntitySchema(entityType, fieldMapping, dataTypes, isValidSchema = true)
        logger.info(s"成功检测到实体类型: ${entityType.name}")
        schema

      case None =>
        logger.warn(s"无法识别实体类型，header: ${header.mkString(", ")}")
        // 返回一个默认的schema，尝试作为通用实体处理
        val dataTypes = buildDefaultDataTypeMapping(header)
        EntitySchema(PersonVertex, fieldMapping, dataTypes, isValidSchema = false)
    }
  }

  /**
   * 检测实体类型
   */
  private def detectEntityType(header: Array[String], fieldMapping: Map[String, Int]): Option[LdbcEntityType] = {
    val headerFields = header.toSet

    // 按优先级尝试匹配各种模式
    val matchScores = entityPatterns.map { case (entityType, pattern) =>
      val score = calculateMatchScore(headerFields, fieldMapping.size, pattern)
      (entityType, score)
    }

    // 找到最佳匹配
    val bestMatch = matchScores.maxBy(_._2)
    if (bestMatch._2 > 0.6) { // 匹配度阈值
      Some(bestMatch._1)
    } else {
      None
    }
  }

  /**
   * 计算匹配分数
   */
  private def calculateMatchScore(headerFields: Set[String], fieldCount: Int, pattern: FieldPattern): Double = {
    var score = 0.0

    // 1. 必需字段匹配度
    val requiredMatches = pattern.requiredFields.count(field =>
      headerFields.exists(_.toLowerCase.contains(field.toLowerCase)))
    val requiredScore = if (pattern.requiredFields.nonEmpty) {
      requiredMatches.toDouble / pattern.requiredFields.size
    } else 0.0

    score += requiredScore * 0.6

    // 2. 字段数量匹配度
    val fieldCountScore = pattern.fieldCount match {
      case Some(expectedCount) =>
        if (fieldCount == expectedCount) 1.0
        else if (Math.abs(fieldCount - expectedCount) <= 2) 0.5
        else 0.0
      case None => 0.5
    }

    score += fieldCountScore * 0.2

    // 3. 唯一标识符匹配度
    val identifierMatches = pattern.uniqueIdentifiers.count(identifier =>
      headerFields.exists(_.toLowerCase.contains(identifier.toLowerCase)))
    val identifierScore = if (pattern.uniqueIdentifiers.nonEmpty) {
      identifierMatches.toDouble / pattern.uniqueIdentifiers.size
    } else 0.0

    score += identifierScore * 0.2

    logger.debug(s"匹配分数计算: required=$requiredScore, fieldCount=$fieldCountScore, identifier=$identifierScore, total=$score")

    score
  }

  /**
   * 基于LDBC raw.scala的精确字段类型映射表
   */
  private val LDBC_FIELD_TYPE_MAPPING = Map(
    // Person fields - 基于 raw.Person
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

    // Forum fields - 基于 raw.Forum，添加驼峰命名支持
    "title" -> StringType,  // 明确指定为StringType
    "creationDate" -> TimestampType,
    "creationdate" -> TimestampType,  // 小写版本
    "deletionDate" -> TimestampType,
    "deletiondate" -> TimestampType,  // 小写版本
    "explicitlyDeleted" -> BooleanType,
    "ModeratorPersonId" -> LongType,
    "moderatorPersonId" -> LongType,  // 驼峰版本
    "moderatorpersonid" -> LongType,  // 小写版本

    // Post fields - 基于 raw.Post
    "content" -> StringType,  // 明确指定为StringType
    "imageFile" -> StringType,
    "length" -> IntegerType,
    "CreatorPersonId" -> LongType,
    "creatorPersonId" -> LongType,
    "ContainerForumId" -> LongType,
    "containerForumId" -> LongType,
    "LocationCountryId" -> LongType,

    // Comment fields - 基于 raw.Comment
    "ParentPostId" -> LongType,
    "parentPostId" -> LongType,
    "ParentCommentId" -> LongType,
    "parentCommentId" -> LongType,

    // 通用ID字段
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
   * 基于字段名推断数据类型
   */
  private def inferDataType(fieldName: String): Option[DataType] = {
    val normalizedFieldName = fieldName.toLowerCase.replaceAll("[^a-z0-9]", "")
    logger.info(s"字段类型推断: 原始字段='$fieldName', 规范化字段='$normalizedFieldName'")

    // 首先查找精确映射
    LDBC_FIELD_TYPE_MAPPING.get(normalizedFieldName) match {
      case Some(dataType) =>
        logger.info(s"字段 '$fieldName' 使用精确类型映射: $dataType")
        Some(dataType)
      case None => {
        logger.info(s"字段 '$fieldName' 未找到精确映射，使用后备推断逻辑")
        // 后备推断逻辑
        val inferredType = if (normalizedFieldName.endsWith("id") || normalizedFieldName.contains("id")) {
          logger.info(s"字段 '$fieldName' 包含'id'，推断为LongType")
          Some(LongType)
        } else if (normalizedFieldName.contains("date") && !normalizedFieldName.contains("birthday")) {
          logger.info(s"字段 '$fieldName' 包含'date'但非'birthday'，推断为TimestampType")
          Some(TimestampType)
        } else if (normalizedFieldName.contains("birthday") || normalizedFieldName.contains("birth")) {
          logger.info(s"字段 '$fieldName' 包含'birthday'，推断为DateType")
          Some(DateType)
        } else if (normalizedFieldName.contains("weight") || normalizedFieldName.contains("score")) {
          logger.info(s"字段 '$fieldName' 包含'weight'或'score'，推断为FloatType")
          Some(FloatType)
        } else if (normalizedFieldName.contains("count") || normalizedFieldName.contains("size") || normalizedFieldName.contains("length")) {
          logger.info(s"字段 '$fieldName' 包含计数相关词汇，推断为IntegerType")
          Some(IntegerType)
        } else if (normalizedFieldName == "title" || normalizedFieldName.contains("name") ||
                   normalizedFieldName.contains("content") || normalizedFieldName.contains("file") ||
                   normalizedFieldName.contains("ip") || normalizedFieldName.contains("browser") ||
                   normalizedFieldName.contains("language") || normalizedFieldName.contains("gender")) {
          // 明确指定这些字段为String类型，避免被误判为其他类型
          logger.info(s"字段 '$fieldName' 匹配字符串模式，推断为StringType")
          Some(StringType)
        } else {
          logger.info(s"字段 '$fieldName' 无法匹配任何模式，默认推断为StringType")
          Some(StringType)
        }

        logger.info(s"字段 '$fieldName' 最终推断类型: $inferredType")
        inferredType
      }
    }
  }

  /**
   * 构建数据类型映射 - 基于LDBC统一映射表
   */
  private def buildDataTypeMapping(header: Array[String], entityType: LdbcEntityType): Map[String, DataType] = {
    header.map { fieldName =>
      val dataType = detectFieldType(fieldName, "").getOrElse(StringType)
      logger.info(s"构建类型映射: $fieldName -> $dataType")
      fieldName -> dataType
    }.toMap
  }

  /**
   * 构建默认数据类型映射 - 用于未识别实体类型的情况
   */
  private def buildDefaultDataTypeMapping(header: Array[String]): Map[String, DataType] = {
    header.map { fieldName =>
      val dataType = inferDataType(fieldName).getOrElse(StringType)
      fieldName -> dataType
    }.toMap
  }

  /**
   * 验证记录是否符合模式
   */
  def validateRecord(record: Array[String], schema: EntitySchema): Boolean = {
    if (!schema.isValidSchema) {
      return true // 对于无效模式，不进行验证
    }

    // 检查字段数量
    if (record.length != schema.fieldMapping.size) {
      logger.warn(s"记录字段数量不匹配: expected=${schema.fieldMapping.size}, actual=${record.length}")
      return false
    }

    // 可以添加更多的数据类型验证
    true
  }

  /**
   * 获取实体的主键字段
   */
  def getPrimaryKeyField(entityType: LdbcEntityType): String = {
    entityType match {
      case _: VertexEntityType => "id"
      case edge: EdgeEntityType =>
        // 对于边，通常是源顶点和目标顶点的组合
        edge match {
          case KnowsEdge => "Person1Id"
          case HasCreatorEdge => "MessageId"
          case ContainerOfEdge => "ForumId"
          case ReplyOfEdge => "CommentId"
          case LikesEdge => "PersonId"
          case HasModeratorEdge => "ForumId"
          case HasMemberEdge => "ForumId"
          case _ => "id"
        }
    }
  }
  def detectFieldType(fieldName: String, sampleValue: String): Option[DataType] = {
    val normalizedFieldName = fieldName.toLowerCase

    // 首先检查精确映射
    LDBC_FIELD_TYPE_MAPPING.get(fieldName) match {
      case Some(dataType) => Some(dataType)
      case None =>
        // 检查小写映射
        LDBC_FIELD_TYPE_MAPPING.get(normalizedFieldName) match {
          case Some(dataType) => Some(dataType)
          case None =>
            // 改进的后备推断逻辑
            if (fieldName == "title" || fieldName == "content") {
              // 明确指定title和content为String类型
              Some(StringType)
            } else if (normalizedFieldName.endsWith("id") || normalizedFieldName.contains("id")) {
              if (normalizedFieldName.contains("tag")) {
                Some(IntegerType)  // TagId通常是Integer
              } else {
                Some(LongType)
              }
            } else if (normalizedFieldName.contains("date") && !normalizedFieldName.contains("birthday")) {
              Some(TimestampType)
            } else if (normalizedFieldName.contains("deleted")) {
              Some(BooleanType)
            } else {
              // 默认为String类型
              Some(StringType)
            }
        }
    }
  }
  /**
   * 获取实体的外键字段
   */
  def getForeignKeyFields(entityType: LdbcEntityType): List[String] = {
    entityType match {
      case PostVertex => List("creatorPersonId", "containerForumId")
      case CommentVertex => List("creatorPersonId", "replyOfPostId", "replyOfCommentId")
      case ForumVertex => List("moderatorPersonId")
      case edge: EdgeEntityType =>
        edge match {
          case KnowsEdge => List("Person1Id", "Person2Id")
          case HasCreatorEdge => List("MessageId", "PersonId")
          case ContainerOfEdge => List("ForumId", "PostId")
          case ReplyOfEdge => List("CommentId", "MessageId")
          case LikesEdge => List("PersonId", "MessageId")
          case HasModeratorEdge => List("ForumId", "PersonId")
          case HasMemberEdge => List("ForumId", "PersonId")
          case _ => List.empty
        }
      case _ => List.empty
    }
  }
}