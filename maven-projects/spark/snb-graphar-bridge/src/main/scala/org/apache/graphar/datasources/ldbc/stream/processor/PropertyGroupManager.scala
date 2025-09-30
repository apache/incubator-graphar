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

import org.apache.graphar.datasources.ldbc.stream.core.{EntitySchemaDetector}
import org.apache.graphar.datasources.ldbc.stream.model.{LdbcEntityType, VertexEntityType, EdgeEntityType, PersonVertex, ForumVertex, PostVertex, CommentVertex, KnowsEdge, HasCreatorEdge, ContainerOfEdge, ReplyOfEdge, LikesEdge, HasModeratorEdge, HasMemberEdge}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * 属性分组管理器
 *
 * 负责按GraphAr列族规范组织属性，实现：
 * 1. 根据实体类型和属性特征进行属性分组
 * 2. 支持自定义属性组策略
 * 3. 优化存储和查询性能
 */
class PropertyGroupManager extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[PropertyGroupManager])

  // 属性分组定义
  private var propertyGroups: Map[String, Set[String]] = Map.empty
  private var fieldToGroup: Map[String, String] = Map.empty

  /**
   * 为指定实体初始化属性分组
   */
  def initializePropertyGroups(schema: EntitySchemaDetector.EntitySchema): Unit = {
    logger.info(s"初始化属性分组: ${schema.entityType.name}")

    val groups = schema.entityType match {
      case vertex: VertexEntityType => createVertexPropertyGroups(vertex, schema)
      case edge: EdgeEntityType => createEdgePropertyGroups(edge, schema)
    }

    propertyGroups = groups
    fieldToGroup = groups.flatMap { case (groupName, fields) =>
      fields.map(_ -> groupName)
    }

    logger.info(s"属性分组初始化完成: ${propertyGroups.size}个分组")
    propertyGroups.foreach { case (groupName, fields) =>
      logger.debug(s"  分组 '$groupName': ${fields.mkString(", ")}")
    }
  }

  /**
   * 创建顶点属性分组 - 基于LDBC raw.scala模型
   */
  private def createVertexPropertyGroups(
      vertex: VertexEntityType,
      schema: EntitySchemaDetector.EntitySchema
  ): Map[String, Set[String]] = {

    val allFields = schema.fieldMapping.keySet
    val groups = mutable.Map[String, Set[String]]()

    vertex match {
      case PersonVertex =>
        // 基于 raw.Person 的结构
        groups += "basic" -> Set("id", "firstName", "lastName", "gender", "birthday", "creationDate", "deletionDate", "explicitlyDeleted")
        groups += "contact" -> Set("locationIP", "browserUsed", "LocationCityId", "language", "email")

      case ForumVertex =>
        // 基于 raw.Forum 的结构
        groups += "basic" -> Set("id", "title", "creationDate", "deletionDate", "explicitlyDeleted")
        groups += "relations" -> Set("ModeratorPersonId")

      case PostVertex =>
        // 基于 raw.Post 的结构
        groups += "basic" -> Set("id", "creationDate", "deletionDate", "explicitlyDeleted")
        groups += "content" -> Set("imageFile", "content", "length", "language")
        groups += "metadata" -> Set("locationIP", "browserUsed", "LocationCountryId")
        groups += "relations" -> Set("CreatorPersonId", "ContainerForumId")

      case CommentVertex =>
        // 基于 raw.Comment 的结构
        groups += "basic" -> Set("id", "creationDate", "deletionDate", "explicitlyDeleted")
        groups += "content" -> Set("content", "length")
        groups += "metadata" -> Set("locationIP", "browserUsed", "LocationCountryId")
        groups += "relations" -> Set("CreatorPersonId", "ParentPostId", "ParentCommentId")

      case _ =>
        // 默认分组策略
        val (basicFields, otherFields) = allFields.partition { field =>
          field == "id" || field.contains("name") || field.contains("title")
        }
        groups += "basic" -> basicFields
        groups += "other" -> otherFields
    }

    // 过滤掉在模式中不存在的字段，使用改进的模糊匹配
    val filteredGroups = groups.map { case (groupName, fieldSet) =>
      groupName -> fieldSet.filter(field => findMatchingField(field, allFields).isDefined)
    }.filter(_._2.nonEmpty)

    logger.debug(s"为顶点 ${vertex.name} 创建了 ${filteredGroups.size} 个属性分组")
    filteredGroups.toMap
  }

  /**
   * 改进的字段匹配算法 - 支持大小写不敏感和命名规范转换
   */
  private def findMatchingField(targetField: String, availableFields: Set[String]): Option[String] = {
    val normalizedTarget = normalizeFieldName(targetField)

    // 1. 精确匹配
    availableFields.find(_.equals(targetField)) match {
      case Some(field) => return Some(field)
      case None =>
    }

    // 2. 大小写不敏感匹配
    availableFields.find(_.toLowerCase == targetField.toLowerCase) match {
      case Some(field) => return Some(field)
      case None =>
    }

    // 3. 标准化后匹配（去除特殊字符后比较）
    availableFields.find(field => normalizeFieldName(field) == normalizedTarget) match {
      case Some(field) => return Some(field)
      case None =>
    }

    // 4. 驼峰命名转换匹配
    val camelCaseVariants = generateCamelCaseVariants(targetField)
    for (variant <- camelCaseVariants) {
      availableFields.find(_.equals(variant)) match {
        case Some(field) => return Some(field)
        case None =>
      }
    }

    None
  }

  /**
   * 标准化字段名称
   */
  private def normalizeFieldName(fieldName: String): String = {
    fieldName.toLowerCase.replaceAll("[^a-z0-9]", "")
  }

  /**
   * 生成驼峰命名的变体
   */
  private def generateCamelCaseVariants(fieldName: String): List[String] = {
    List(
      fieldName,
      fieldName.toLowerCase,
      fieldName.capitalize,
      toCamelCase(fieldName),
      toSnakeCase(fieldName)
    ).distinct
  }

  /**
   * 转换为驼峰命名
   */
  private def toCamelCase(fieldName: String): String = {
    if (fieldName.contains("_")) {
      fieldName.split("_").zipWithIndex.map { case (part, index) =>
        if (index == 0) part.toLowerCase else part.capitalize
      }.mkString
    } else if (fieldName.head.isUpper && fieldName.tail.exists(_.isUpper)) {
      // PascalCase转camelCase
      fieldName.head.toLower + fieldName.tail
    } else {
      fieldName
    }
  }

  /**
   * 转换为snake_case命名
   */
  private def toSnakeCase(fieldName: String): String = {
    fieldName.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase
  }

  /**
   * 创建边属性分组
   */
  private def createEdgePropertyGroups(
      edge: EdgeEntityType,
      schema: EntitySchemaDetector.EntitySchema
  ): Map[String, Set[String]] = {

    val allFields = schema.fieldMapping.keySet
    val groups = mutable.Map[String, Set[String]]()

    edge match {
      case KnowsEdge =>
        groups += "basic" -> Set("Person1Id", "Person2Id", "creationDate")
        groups += "weight" -> Set("weight")

      case HasCreatorEdge =>
        groups += "basic" -> Set("MessageId", "PersonId")

      case ContainerOfEdge =>
        groups += "basic" -> Set("ForumId", "PostId")

      case ReplyOfEdge =>
        groups += "basic" -> Set("CommentId", "MessageId")

      case LikesEdge =>
        groups += "basic" -> Set("PersonId", "MessageId", "creationDate")

      case HasModeratorEdge =>
        groups += "basic" -> Set("ForumId", "PersonId")

      case HasMemberEdge =>
        groups += "basic" -> Set("ForumId", "PersonId", "joinDate")

      case _ =>
        // 默认边分组策略：所有字段放在basic分组
        groups += "basic" -> allFields
    }

    // 确保所有字段都被分组
    val groupedFields = groups.values.flatten.toSet
    val ungroupedFields = allFields -- groupedFields
    if (ungroupedFields.nonEmpty) {
      groups += "misc" -> ungroupedFields
    }

    groups.toMap
  }

  /**
   * 按属性分组组织记录
   */
  def groupByProperties(
      records: List[Array[String]],
      schema: EntitySchemaDetector.EntitySchema
  ): Map[String, List[Array[String]]] = {

    if (propertyGroups.isEmpty) {
      logger.warn("属性分组未初始化，使用默认分组")
      return Map("default" -> records)
    }

    val result = mutable.Map[String, mutable.ListBuffer[Array[String]]]()

    // 为每个属性组初始化结果集
    propertyGroups.keys.foreach { groupName =>
      result += groupName -> mutable.ListBuffer[Array[String]]()
    }

    // 按分组提取记录数据
    records.foreach { record =>
      propertyGroups.foreach { case (groupName, fieldNames) =>
        val groupRecord = extractGroupFields(record, fieldNames, schema)
        result(groupName) += groupRecord
      }
    }

    result.mapValues(_.toList).toMap
  }

  /**
   * 提取指定分组的字段 - 使用改进的字段匹配
   */
  private def extractGroupFields(
      record: Array[String],
      fieldNames: Set[String],
      schema: EntitySchemaDetector.EntitySchema
  ): Array[String] = {

    val allFields = schema.fieldMapping.keySet

    val groupFields = fieldNames.toSeq.sorted.map { fieldName =>
      // 首先尝试直接获取字段索引
      schema.getFieldIndex(fieldName) match {
        case Some(index) if index < record.length => record(index)
        case Some(index) =>
          logger.warn(s"字段索引超出范围: field=$fieldName, index=$index, recordLength=${record.length}")
          ""
        case None =>
          // 使用改进的字段匹配查找对应字段
          findMatchingField(fieldName, allFields) match {
            case Some(matchedField) =>
              schema.getFieldIndex(matchedField) match {
                case Some(index) if index < record.length => record(index)
                case Some(index) =>
                  logger.warn(s"字段索引超出范围: field=$matchedField (matched for $fieldName), index=$index, recordLength=${record.length}")
                  ""
                case None =>
                  logger.warn(s"找到匹配字段但无法获取索引: matched=$matchedField for original=$fieldName")
                  ""
              }
            case None =>
              logger.warn(s"未找到字段: $fieldName (在可用字段中: ${allFields.mkString(", ")})")
              ""
          }
      }
    }

    groupFields.toArray
  }

  /**
   * 获取字段所属的属性组
   */
  def getFieldGroup(fieldName: String): Option[String] = {
    fieldToGroup.get(fieldName)
  }

  /**
   * 获取指定分组的所有字段
   */
  def getGroupFields(groupName: String): Set[String] = {
    propertyGroups.getOrElse(groupName, Set.empty)
  }

  /**
   * 获取所有属性分组
   */
  def getAllGroups(): Map[String, Set[String]] = propertyGroups

  /**
   * 获取属性分组统计信息
   */
  def getGroupingStatistics(): GroupingStatistics = {
    val totalFields = propertyGroups.values.map(_.size).sum
    val groupCount = propertyGroups.size
    val avgFieldsPerGroup = if (groupCount > 0) totalFields.toDouble / groupCount else 0.0

    GroupingStatistics(
      totalFields = totalFields,
      groupCount = groupCount,
      avgFieldsPerGroup = avgFieldsPerGroup,
      groups = propertyGroups.mapValues(_.size).toMap
    )
  }
}

/**
 * 属性分组统计信息
 */
case class GroupingStatistics(
  totalFields: Int,
  groupCount: Int,
  avgFieldsPerGroup: Double,
  groups: Map[String, Int] // 分组名 -> 字段数量
)