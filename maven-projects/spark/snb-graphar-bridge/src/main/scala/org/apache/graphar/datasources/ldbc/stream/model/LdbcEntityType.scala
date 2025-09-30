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

package org.apache.graphar.datasources.ldbc.stream.model

/**
 * LDBC实体类型系统
 *
 * 支持LDBC SNB的所有静态和动态实体类型，包括顶点和边实体。
 * 提供统一的类型标识和属性定义。
 */
sealed trait LdbcEntityType {
  def name: String
  def isVertex: Boolean
  def isEdge: Boolean = !isVertex
  def tableName: String
  def expectedFieldCount: Int
}

/**
 * 顶点实体类型
 */
sealed trait VertexEntityType extends LdbcEntityType {
  override val isVertex: Boolean = true
}

/**
 * 边实体类型
 */
sealed trait EdgeEntityType extends LdbcEntityType {
  override val isVertex: Boolean = false
  def srcVertexType: VertexEntityType
  def dstVertexType: VertexEntityType
}

/**
 * 静态顶点实体（现有支持）
 */
case object PersonVertex extends VertexEntityType {
  override val name: String = "Person"
  override val tableName: String = "person"
  override val expectedFieldCount: Int = 11
}

case object PlaceVertex extends VertexEntityType {
  override val name: String = "Place"
  override val tableName: String = "place"
  override val expectedFieldCount: Int = 4
}

case object OrganisationVertex extends VertexEntityType {
  override val name: String = "Organisation"
  override val tableName: String = "organisation"
  override val expectedFieldCount: Int = 3
}

case object TagVertex extends VertexEntityType {
  override val name: String = "Tag"
  override val tableName: String = "tag"
  override val expectedFieldCount: Int = 3
}

case object TagClassVertex extends VertexEntityType {
  override val name: String = "TagClass"
  override val tableName: String = "tagclass"
  override val expectedFieldCount: Int = 3
}

case object UniversityVertex extends VertexEntityType {
  override val name: String = "University"
  override val tableName: String = "university"
  override val expectedFieldCount: Int = 3
}

/**
 * 动态顶点实体（目标新增支持）
 */
case object ForumVertex extends VertexEntityType {
  override val name: String = "Forum"
  override val tableName: String = "forum"
  override val expectedFieldCount: Int = 4
}

case object PostVertex extends VertexEntityType {
  override val name: String = "Post"
  override val tableName: String = "post"
  override val expectedFieldCount: Int = 8
}

case object CommentVertex extends VertexEntityType {
  override val name: String = "Comment"
  override val tableName: String = "comment"
  override val expectedFieldCount: Int = 7
}

/**
 * 静态边实体（现有支持）
 */
case object KnowsEdge extends EdgeEntityType {
  override val name: String = "knows"
  override val tableName: String = "person_knows_person"
  override val expectedFieldCount: Int = 4
  override val srcVertexType: VertexEntityType = PersonVertex
  override val dstVertexType: VertexEntityType = PersonVertex
}

case object HasInterestEdge extends EdgeEntityType {
  override val name: String = "hasInterest"
  override val tableName: String = "person_hasInterest_tag"
  override val expectedFieldCount: Int = 2
  override val srcVertexType: VertexEntityType = PersonVertex
  override val dstVertexType: VertexEntityType = TagVertex
}

case object WorkAtEdge extends EdgeEntityType {
  override val name: String = "workAt"
  override val tableName: String = "person_workAt_organisation"
  override val expectedFieldCount: Int = 3
  override val srcVertexType: VertexEntityType = PersonVertex
  override val dstVertexType: VertexEntityType = OrganisationVertex
}

case object StudyAtEdge extends EdgeEntityType {
  override val name: String = "studyAt"
  override val tableName: String = "person_studyAt_university"
  override val expectedFieldCount: Int = 3
  override val srcVertexType: VertexEntityType = PersonVertex
  override val dstVertexType: VertexEntityType = UniversityVertex
}

case object IsLocatedInEdge extends EdgeEntityType {
  override val name: String = "isLocatedIn"
  override val tableName: String = "person_isLocatedIn_place"
  override val expectedFieldCount: Int = 2
  override val srcVertexType: VertexEntityType = PersonVertex
  override val dstVertexType: VertexEntityType = PlaceVertex
}

/**
 * 动态边实体（目标新增支持）
 */
case object HasCreatorEdge extends EdgeEntityType {
  override val name: String = "hasCreator"
  override val tableName: String = "message_hasCreator_person"
  override val expectedFieldCount: Int = 2
  override val srcVertexType: VertexEntityType = PostVertex // 可能是Post或Comment
  override val dstVertexType: VertexEntityType = PersonVertex
}

case object ContainerOfEdge extends EdgeEntityType {
  override val name: String = "containerOf"
  override val tableName: String = "forum_containerOf_post"
  override val expectedFieldCount: Int = 2
  override val srcVertexType: VertexEntityType = ForumVertex
  override val dstVertexType: VertexEntityType = PostVertex
}

case object ReplyOfEdge extends EdgeEntityType {
  override val name: String = "replyOf"
  override val tableName: String = "comment_replyOf_message"
  override val expectedFieldCount: Int = 2
  override val srcVertexType: VertexEntityType = CommentVertex
  override val dstVertexType: VertexEntityType = PostVertex // 可能是Post或Comment
}

case object LikesEdge extends EdgeEntityType {
  override val name: String = "likes"
  override val tableName: String = "person_likes_message"
  override val expectedFieldCount: Int = 3
  override val srcVertexType: VertexEntityType = PersonVertex
  override val dstVertexType: VertexEntityType = PostVertex // 可能是Post或Comment
}

case object HasModeratorEdge extends EdgeEntityType {
  override val name: String = "hasModerator"
  override val tableName: String = "forum_hasModerator_person"
  override val expectedFieldCount: Int = 2
  override val srcVertexType: VertexEntityType = ForumVertex
  override val dstVertexType: VertexEntityType = PersonVertex
}

case object HasMemberEdge extends EdgeEntityType {
  override val name: String = "hasMember"
  override val tableName: String = "forum_hasMember_person"
  override val expectedFieldCount: Int = 3
  override val srcVertexType: VertexEntityType = ForumVertex
  override val dstVertexType: VertexEntityType = PersonVertex
}

/**
 * 实体类型工具对象
 */
object LdbcEntityType {

  /**
   * 所有静态实体类型（现有支持）
   */
  val staticVertices: Set[VertexEntityType] = Set(
    PersonVertex, PlaceVertex, OrganisationVertex, TagVertex, TagClassVertex, UniversityVertex
  )

  val staticEdges: Set[EdgeEntityType] = Set(
    KnowsEdge, HasInterestEdge, WorkAtEdge, StudyAtEdge, IsLocatedInEdge
  )

  /**
   * 所有动态实体类型（目标新增支持）
   */
  val dynamicVertices: Set[VertexEntityType] = Set(
    ForumVertex, PostVertex, CommentVertex
  )

  val dynamicEdges: Set[EdgeEntityType] = Set(
    HasCreatorEdge, ContainerOfEdge, ReplyOfEdge, LikesEdge, HasModeratorEdge, HasMemberEdge
  )

  /**
   * 所有实体类型
   */
  val allVertices: Set[VertexEntityType] = staticVertices ++ dynamicVertices
  val allEdges: Set[EdgeEntityType] = staticEdges ++ dynamicEdges
  val allEntities: Set[LdbcEntityType] = allVertices ++ allEdges

  /**
   * 根据名称查找实体类型
   */
  def findByName(name: String): Option[LdbcEntityType] = {
    allEntities.find(_.name.equalsIgnoreCase(name))
  }

  /**
   * 根据表名查找实体类型
   */
  def findByTableName(tableName: String): Option[LdbcEntityType] = {
    allEntities.find(_.tableName.equalsIgnoreCase(tableName))
  }

  /**
   * 获取实体覆盖率统计
   */
  def getCoverageStats(): (Double, Double, Double) = {
    val totalEntities = allEntities.size
    val staticCoverage = (staticVertices.size + staticEdges.size).toDouble / totalEntities * 100
    val dynamicCoverage = (dynamicVertices.size + dynamicEdges.size).toDouble / totalEntities * 100
    val totalCoverage = staticCoverage + dynamicCoverage

    (staticCoverage, dynamicCoverage, totalCoverage)
  }

  /**
   * 检查是否为动态实体
   */
  def isDynamic(entityType: LdbcEntityType): Boolean = {
    entityType match {
      case vertex: VertexEntityType => dynamicVertices.contains(vertex)
      case edge: EdgeEntityType => dynamicEdges.contains(edge)
      case _ => false
    }
  }

  /**
   * 检查是否为静态实体
   */
  def isStatic(entityType: LdbcEntityType): Boolean = {
    entityType match {
      case vertex: VertexEntityType => staticVertices.contains(vertex)
      case edge: EdgeEntityType => staticEdges.contains(edge)
      case _ => false
    }
  }
}