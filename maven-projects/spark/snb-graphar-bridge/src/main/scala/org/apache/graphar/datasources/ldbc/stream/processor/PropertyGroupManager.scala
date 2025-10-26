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
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
 * Property groupmanager
 *
 * Responsible for organizing properties into groups according to GraphAr column
 * family specifications, implements:
 *   1. Perform property grouping based on entity type and property
 *      characteristics 2. SupportCustomPropertyGroupPolicy 3. Optimize storage
 *      and query performance
 */
class PropertyGroupManager extends Serializable {

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[PropertyGroupManager])

  // Property group definitions
  private var propertyGroups: Map[String, Set[String]] = Map.empty
  private var fieldToGroup: Map[String, String] = Map.empty

  /**
   * Initialize property groups for specified entity
   */
  def initializePropertyGroups(
      schema: EntitySchemaDetector.EntitySchema
  ): Unit = {
    logger.info(s"Initializing property groups: ${schema.entityType.name}")

    val groups = schema.entityType match {
      case vertex: VertexEntityType =>
        createVertexPropertyGroups(vertex, schema)
      case edge: EdgeEntityType => createEdgePropertyGroups(edge, schema)
    }

    propertyGroups = groups
    fieldToGroup = groups.flatMap { case (groupName, fields) =>
      fields.map(_ -> groupName)
    }

    logger.info(
      s"Property grouping initialization completed: ${propertyGroups.size} groups"
    )
    propertyGroups.foreach { case (groupName, fields) =>
      logger.debug(s" Group '$groupName': ${fields.mkString(", ")}")
    }
  }

  /**
   * Create vertex property groups - based on LDBC raw.scala model
   */
  private def createVertexPropertyGroups(
      vertex: VertexEntityType,
      schema: EntitySchemaDetector.EntitySchema
  ): Map[String, Set[String]] = {

    val allFields = schema.fieldMapping.keySet
    val groups = mutable.Map[String, Set[String]]()

    vertex match {
      case PersonVertex =>
        // Based on raw.Person structure
        groups += "basic" -> Set(
          "id",
          "firstName",
          "lastName",
          "gender",
          "birthday",
          "creationDate",
          "deletionDate",
          "explicitlyDeleted"
        )
        groups += "contact" -> Set(
          "locationIP",
          "browserUsed",
          "LocationCityId",
          "language",
          "email"
        )

      case ForumVertex =>
        // Based on raw.Forum structure
        groups += "basic" -> Set(
          "id",
          "title",
          "creationDate",
          "deletionDate",
          "explicitlyDeleted"
        )
        groups += "relations" -> Set("ModeratorPersonId")

      case PostVertex =>
        // Based on raw.Post structure
        groups += "basic" -> Set(
          "id",
          "creationDate",
          "deletionDate",
          "explicitlyDeleted"
        )
        groups += "content" -> Set("imageFile", "content", "length", "language")
        groups += "metadata" -> Set(
          "locationIP",
          "browserUsed",
          "LocationCountryId"
        )
        groups += "relations" -> Set("CreatorPersonId", "ContainerForumId")

      case CommentVertex =>
        // Based on raw.Comment structure
        groups += "basic" -> Set(
          "id",
          "creationDate",
          "deletionDate",
          "explicitlyDeleted"
        )
        groups += "content" -> Set("content", "length")
        groups += "metadata" -> Set(
          "locationIP",
          "browserUsed",
          "LocationCountryId"
        )
        groups += "relations" -> Set(
          "CreatorPersonId",
          "ParentPostId",
          "ParentCommentId"
        )

      case _ =>
        // DefaultGroupPolicy
        val (basicFields, otherFields) = allFields.partition { field =>
          field == "id" || field.contains("name") || field.contains("title")
        }
        groups += "basic" -> basicFields
        groups += "other" -> otherFields
    }

    // Filter out fields not present in schema, use improved fuzzy matching
    val filteredGroups = groups
      .map { case (groupName, fieldSet) =>
        groupName -> fieldSet.filter(field =>
          findMatchingField(field, allFields).isDefined
        )
      }
      .filter(_._2.nonEmpty)

    logger.debug(
      s"Created ${filteredGroups.size} property groups for vertex ${vertex.name}"
    )
    filteredGroups.toMap
  }

  /**
   * Improved field matching algorithm - support case-insensitive and naming
   * convention conversion
   */
  private def findMatchingField(
      targetField: String,
      availableFields: Set[String]
  ): Option[String] = {
    val normalizedTarget = normalizeFieldName(targetField)

    // 1. Exact match
    availableFields.find(_.equals(targetField)) match {
      case Some(field) => return Some(field)
      case None        =>
    }

    // 2. Case-insensitive match
    availableFields.find(_.toLowerCase == targetField.toLowerCase) match {
      case Some(field) => return Some(field)
      case None        =>
    }

    // 3. Normalized match (remove special characters for comparison)
    availableFields.find(field =>
      normalizeFieldName(field) == normalizedTarget
    ) match {
      case Some(field) => return Some(field)
      case None        =>
    }

    // 4. Camel case conversion match
    val camelCaseVariants = generateCamelCaseVariants(targetField)
    for (variant <- camelCaseVariants) {
      availableFields.find(_.equals(variant)) match {
        case Some(field) => return Some(field)
        case None        =>
      }
    }

    None
  }

  /**
   * Normalize field name
   */
  private def normalizeFieldName(fieldName: String): String = {
    fieldName.toLowerCase.replaceAll("[^a-z0-9]", "")
  }

  /**
   * Generate camel case variants
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
   * Convert to camel case
   */
  private def toCamelCase(fieldName: String): String = {
    if (fieldName.contains("_")) {
      fieldName
        .split("_")
        .zipWithIndex
        .map { case (part, index) =>
          if (index == 0) part.toLowerCase else part.capitalize
        }
        .mkString
    } else if (fieldName.head.isUpper && fieldName.tail.exists(_.isUpper)) {
      // PascalCase to camelCase
      fieldName.head.toLower + fieldName.tail
    } else {
      fieldName
    }
  }

  /**
   * Convert to snake_case naming
   */
  private def toSnakeCase(fieldName: String): String = {
    fieldName.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase
  }

  /**
   * CreateEdgeProperty group
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
        // Default edge grouping policy: place all fields in basic group
        groups += "basic" -> allFields
    }

    // Ensure all fields are grouped
    val groupedFields = groups.values.flatten.toSet
    val ungroupedFields = allFields -- groupedFields
    if (ungroupedFields.nonEmpty) {
      groups += "misc" -> ungroupedFields
    }

    groups.toMap
  }

  /**
   * Organize records by property groups
   */
  def groupByProperties(
      records: List[Array[String]],
      schema: EntitySchemaDetector.EntitySchema
  ): Map[String, List[Array[String]]] = {

    if (propertyGroups.isEmpty) {
      logger.warn("Property grouping not initialized, using default grouping")
      return Map("default" -> records)
    }

    val result = mutable.Map[String, mutable.ListBuffer[Array[String]]]()

    // Initialize result set for each property group
    propertyGroups.keys.foreach { groupName =>
      result += groupName -> mutable.ListBuffer[Array[String]]()
    }

    // Extract record data by group
    records.foreach { record =>
      propertyGroups.foreach { case (groupName, fieldNames) =>
        val groupRecord = extractGroupFields(record, fieldNames, schema)
        result(groupName) += groupRecord
      }
    }

    result.mapValues(_.toList).toMap
  }

  /**
   * Extract fields for specified group - use improved field matching
   */
  private def extractGroupFields(
      record: Array[String],
      fieldNames: Set[String],
      schema: EntitySchemaDetector.EntitySchema
  ): Array[String] = {

    val allFields = schema.fieldMapping.keySet

    val groupFields = fieldNames.toSeq.sorted.map { fieldName =>
      // First try to get field index directly
      schema.getFieldIndex(fieldName) match {
        case Some(index) if index < record.length => record(index)
        case Some(index) =>
          logger.warn(
            s"Field index out of range: field=$fieldName, index=$index, recordLength=${record.length}"
          )
          ""
        case None =>
          // Use improved field matching to find corresponding field
          findMatchingField(fieldName, allFields) match {
            case Some(matchedField) =>
              schema.getFieldIndex(matchedField) match {
                case Some(index) if index < record.length => record(index)
                case Some(index) =>
                  logger.warn(
                    s"Field index out of range: field=$matchedField (matched for $fieldName), index=$index, recordLength=${record.length}"
                  )
                  ""
                case None =>
                  logger.warn(
                    s"Found matching field but unable to get index: matched=$matchedField for original=$fieldName"
                  )
                  ""
              }
            case None =>
              logger.warn(
                s"Field not found: $fieldName (available fields: ${allFields.mkString(", ")})"
              )
              ""
          }
      }
    }

    groupFields.toArray
  }

  /**
   * Get property group to which field belongs
   */
  def getFieldGroup(fieldName: String): Option[String] = {
    fieldToGroup.get(fieldName)
  }

  /**
   * Get all fields for specified group
   */
  def getGroupFields(groupName: String): Set[String] = {
    propertyGroups.getOrElse(groupName, Set.empty)
  }

  /**
   * Getall properties group
   */
  def getAllGroups(): Map[String, Set[String]] = propertyGroups

  /**
   * GetProperty groupStatistics
   */
  def getGroupingStatistics(): GroupingStatistics = {
    val totalFields = propertyGroups.values.map(_.size).sum
    val groupCount = propertyGroups.size
    val avgFieldsPerGroup =
      if (groupCount > 0) totalFields.toDouble / groupCount else 0.0

    GroupingStatistics(
      totalFields = totalFields,
      groupCount = groupCount,
      avgFieldsPerGroup = avgFieldsPerGroup,
      groups = propertyGroups.mapValues(_.size).toMap
    )
  }
}

/**
 * Property groupStatistics
 */
case class GroupingStatistics(
    totalFields: Int,
    groupCount: Int,
    avgFieldsPerGroup: Double,
    groups: Map[String, Int] // Group name -> field count
)
