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

package org.apache.graphar.datasources.ldbc.stream.output

import ldbc.snb.datagen.io.raw.RecordOutputStream
import ldbc.snb.datagen.entities.dynamic.{Forum}
import ldbc.snb.datagen.entities.dynamic.messages.{Post, Comment, Photo}
import ldbc.snb.datagen.entities.dynamic.relations.Like
import ldbc.snb.datagen.generator.generators.{GenActivity, GenWall}
import org.apache.graphar.datasources.ldbc.stream.core.{EntitySchemaDetector}
import org.apache.graphar.datasources.ldbc.stream.processor.PropertyGroupManager
import org.apache.graphar.graph.GraphWriter
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType, DateType}
import org.slf4j.{Logger, LoggerFactory}
import org.javatuples.{Pair, Triplet}

import java.util.{List => JList}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * GraphAr输出流，继承LDBC的RecordOutputStream接口
 * 处理字符串列表数据并转换为GraphAr格式
 */
class GraphArActivityOutputStream(
                                   private val outputPath: String,
                                   private val graphName: String,
                                   private val fileType: String = "parquet"
                                 )(@transient implicit val spark: SparkSession) extends RecordOutputStream[JList[String]] with Serializable {

  @transient private val logger: Logger = LoggerFactory.getLogger(classOf[GraphArActivityOutputStream])
  private val propertyGroupManager = new PropertyGroupManager()

  // 懒初始化SparkSession以处理反序列化
  @transient private lazy val sparkSession: SparkSession = {
    if (spark != null) spark else SparkSession.active
  }

  // 数据缓冲区
  private val dataBuffer = mutable.ArrayBuffer[JList[String]]()
  private var headerBuffer: Option[JList[String]] = None
  private var entityType: Option[String] = None
  private var detectedSchema: Option[EntitySchemaDetector.EntitySchema] = None
  private var batchCounter = 0

  /**
   * 写入header信息
   */
  def writeHeader(header: JList[String]): Unit = {
    logger.info(s"接收到header: ${header.asScala.mkString(", ")}")
    headerBuffer = Some(header)

    // 检测实体类型
    val schema = EntitySchemaDetector.detectSchema(header.asScala.toArray)
    detectedSchema = Some(schema)
    entityType = Some(schema.entityType.name)

    // 初始化属性分组
    propertyGroupManager.initializePropertyGroups(schema)

    logger.info(s"检测到实体类型: ${schema.entityType.name}")
  }

  /**
   * 获取符合GraphAr标准的路径
   */
  private def getStandardGraphArPath(entityType: String): String = {
    entityType.toLowerCase match {
      // 顶点实体
      case "person" | "forum" | "post" | "comment" => s"$outputPath/vertex/$entityType"
      // 边实体 - 需要根据实际的边类型来确定
      case "likes" => s"$outputPath/edge/Person_likes_Post"  // 这里需要更具体的处理
      case "hastag" => s"$outputPath/edge/Post_hasTag_Tag"
      case "hasmember" => s"$outputPath/edge/Forum_hasMember_Person"
      case _ => s"$outputPath/$entityType"  // 回退到原有逻辑
    }
  }

  /**
   * 处理真实的LDBC活动数据（基于ActivityOutputStream架构）
   */
  def processLdbcActivity(activity: GenActivity): Unit = {
    logger.info("开始处理真实的LDBC活动数据")

    try {
      // 参考ActivityOutputStream.write()的实现
      writePostWall(activity.genWall)

      // 处理群组数据
      import scala.collection.JavaConverters._
      activity.genGroups.forEach(groupWall => writePostWall(groupWall))

      // 处理相册数据
      writeAlbumWall(activity.genAlbums)

      logger.info("LDBC活动数据处理完成")

    } catch {
      case e: Exception =>
        logger.error("处理LDBC活动数据时出错", e)
        throw e
    }
  }

  /**
   * 处理Post墙数据（参考ActivityOutputStream:35-48）
   */
  private def writePostWall(genWall: GenWall[Triplet[Post, java.util.stream.Stream[Like], java.util.stream.Stream[Pair[Comment, java.util.stream.Stream[Like]]]]]): Unit = {
    import scala.collection.JavaConverters._

    logger.debug("处理Post墙数据")

    // 使用GenWall.inner访问真实数据结构
    genWall.inner.forEach { triplet =>
      val forum = triplet.getValue0  // Forum
      val memberships = triplet.getValue1  // Stream<ForumMembership>
      val posts = triplet.getValue2  // Stream<Triplet<Post, Stream<Like>, Stream<Pair<Comment, Stream<Like>>>>>

      // 写入Forum实体
      writeForum(forum)

      // 写入Forum成员关系
      memberships.forEach(membership => writeForumMembership(membership))

      // 处理Posts及其相关的Comments和Likes
      posts.forEach { postTriplet =>
        val post = postTriplet.getValue0  // Post
        val postLikes = postTriplet.getValue1  // Stream<Like>
        val comments = postTriplet.getValue2  // Stream<Pair<Comment, Stream<Like>>>

        // 写入Post实体
        writePost(post)

        // 写入Post的Like关系
        postLikes.forEach(like => writeLike(like))

        // 处理Comments及其Likes
        comments.forEach { commentPair =>
          val comment = commentPair.getValue0  // Comment
          val commentLikes = commentPair.getValue1  // Stream<Like>

          writeComment(comment)
          commentLikes.forEach(like => writeLike(like))
        }
      }
    }
  }

  /**
   * 处理相册墙数据（参考ActivityOutputStream:50-59）
   */
  private def writeAlbumWall(genAlbums: GenWall[Pair[Photo, java.util.stream.Stream[Like]]]): Unit = {
    import scala.collection.JavaConverters._

    logger.debug("处理相册数据")

    genAlbums.inner.forEach { triplet =>
      val forum = triplet.getValue0  // Forum (相册)
      val memberships = triplet.getValue1  // Stream<ForumMembership>
      val photos = triplet.getValue2  // Stream<Pair<Photo, Stream<Like>>>

      // 写入相册Forum实体
      writeForum(forum)

      // 写入相册成员关系
      memberships.forEach(membership => writeForumMembership(membership))

      // 处理Photos及其Likes
      photos.forEach { photoPair =>
        val photo = photoPair.getValue0  // Photo
        val photoLikes = photoPair.getValue1  // Stream<Like>

        writePhoto(photo)
        photoLikes.forEach(like => writeLike(like))
      }
    }
  }

  /**
   * 写入Forum实体（参考ActivityOutputStream Forum处理）
   */
  private def writeForum(forum: Forum): Unit = {
    import scala.collection.JavaConverters._

    logger.debug(s"写入Forum实体: ${forum.getId}")

    // 构造Forum header（基于LDBC标准）
    val forumHeader = List("id", "title", "creationDate", "moderatorPersonId")

    // 构造Forum数据记录
    val forumRecord = List(
      forum.getId.toString,
      forum.getTitle,
      formatTimestamp(forum.getCreationDate),
      forum.getModerator.getAccountId.toString
    ).asJava

    // 处理Forum实体
    processEntityRecord("Forum", forumHeader.asJava, forumRecord)
  }

  /**
   * 写入Post实体（参考ActivityOutputStream Post处理）
   */
  private def writePost(post: Post): Unit = {
    import scala.collection.JavaConverters._

    logger.debug(s"写入Post实体: ${post.getMessageId}")

    // 构造Post header（基于LDBC标准）
    val postHeader = List(
      "id", "imageFile", "creationDate", "locationIP", "browserUsed",
      "language", "content", "length", "authorPersonId", "forumId", "countryId"
    )

    // 构造Post数据记录
    val postRecord = List(
      post.getMessageId.toString,
      "", // imageFile - Post通常没有图片
      formatTimestamp(post.getCreationDate),
      post.getIpAddress.toString,
      post.getBrowserId.toString,
      post.getLanguage.toString,
      post.getContent,
      post.getContent.length.toString,
      post.getAuthor.getAccountId.toString,
      post.getForumId.toString,
      post.getCountryId.toString
    ).asJava

    // 处理Post实体
    processEntityRecord("Post", postHeader.asJava, postRecord)
  }

  /**
   * 写入Comment实体（参考ActivityOutputStream Comment处理）
   */
  private def writeComment(comment: Comment): Unit = {
    import scala.collection.JavaConverters._

    logger.debug(s"写入Comment实体: ${comment.getMessageId}")

    // 构造Comment header（基于LDBC标准）
    val commentHeader = List(
      "id", "creationDate", "locationIP", "browserUsed",
      "content", "length", "authorPersonId", "countryId", "replyToPostId", "replyToCommentId"
    )

    // 构造Comment数据记录
    val commentRecord = List(
      comment.getMessageId.toString,
      formatTimestamp(comment.getCreationDate),
      comment.getIpAddress.toString,
      comment.getBrowserId.toString,
      comment.getContent,
      comment.getContent.length.toString,
      comment.getAuthor.getAccountId.toString,
      comment.getCountryId.toString,
      if (comment.getRootPostId != -1) comment.getRootPostId.toString else "", // replyToPostId
      if (comment.getParentMessageId != -1 && comment.getParentMessageId != comment.getRootPostId) comment.getParentMessageId.toString else "" // replyToCommentId
    ).asJava

    // 处理Comment实体
    processEntityRecord("Comment", commentHeader.asJava, commentRecord)
  }

  /**
   * 写入Like关系（参考ActivityOutputStream Like处理）
   */
  private def writeLike(like: Like): Unit = {
    import scala.collection.JavaConverters._

    logger.debug(s"写入Like关系: Person ${like.getPerson} likes ${like.getMessageId}")

    // 构造Like关系header（基于LDBC标准）
    val likeHeader = List("personId", "messageId", "creationDate")

    // 构造Like关系数据记录
    val likeRecord = List(
      like.getPerson.toString,
      like.getMessageId.toString,
      formatTimestamp(like.getCreationDate)
    ).asJava

    // 处理Like关系
    processEntityRecord("likes", likeHeader.asJava, likeRecord)
  }

  /**
   * 写入ForumMembership关系
   */
  private def writeForumMembership(membership: ldbc.snb.datagen.entities.dynamic.relations.ForumMembership): Unit = {
    import scala.collection.JavaConverters._

    logger.debug(s"写入Forum成员关系: Person ${membership.getPerson.getAccountId} member of Forum ${membership.getForumId}")

    // 构造ForumMembership关系header
    val membershipHeader = List("forumId", "personId", "joinDate")

    // 构造ForumMembership关系数据记录
    val membershipRecord = List(
      membership.getForumId.toString,
      membership.getPerson.getAccountId.toString,
      formatTimestamp(membership.getCreationDate)
    ).asJava

    // 处理ForumMembership关系
    processEntityRecord("hasMember", membershipHeader.asJava, membershipRecord)
  }

  /**
   * 写入Photo实体
   */
  private def writePhoto(photo: Photo): Unit = {
    import scala.collection.JavaConverters._

    logger.debug(s"写入Photo实体: ${photo.getMessageId}")

    // 构造Photo header
    val photoHeader = List(
      "id", "imageFile", "creationDate", "locationIP", "browserUsed",
      "authorPersonId", "forumId", "countryId"
    )

    // 构造Photo数据记录
    val photoRecord = List(
      photo.getMessageId.toString,
      photo.getContent, // Photo的content是imageFile路径
      formatTimestamp(photo.getCreationDate),
      photo.getIpAddress.toString,
      photo.getBrowserId.toString,
      photo.getAuthor.getAccountId.toString,
      photo.getForumId.toString,
      photo.getCountryId.toString
    ).asJava

    // 处理Photo实体
    processEntityRecord("Photo", photoHeader.asJava, photoRecord)
  }

  /**
   * 统一的实体记录处理方法（直接处理，避免递归创建）
   */
  private def processEntityRecord(entityType: String, header: JList[String], record: JList[String]): Unit = {
    // 获取或初始化该实体类型的数据缓冲区
    val entityBuffer = entityBuffers.getOrElseUpdate(entityType, mutable.ArrayBuffer[JList[String]]())
    val entityHeaderMap = entityHeaders.getOrElseUpdate(entityType, None)

    // 设置header（如果还没有设置）
    if (entityHeaderMap.isEmpty) {
      entityHeaders(entityType) = Some(header)
      logger.debug(s"设置 $entityType 实体的header: ${header.asScala.mkString(", ")}")
    }

    // 添加数据记录到缓冲区
    entityBuffer += record
    logger.debug(s"添加 $entityType 实体记录，当前缓冲区大小: ${entityBuffer.size}")

    // 批量处理数据（每1000条处理一次）
    if (entityBuffer.size >= 1000) {
      flushEntityBuffer(entityType)
    }
  }

  /**
   * 刷新特定实体类型的缓冲区
   */
  private def flushEntityBuffer(entityType: String): Unit = {
    val buffer = entityBuffers.get(entityType)
    val headerOpt = entityHeaders.get(entityType).flatten

    if (buffer.isDefined && headerOpt.isDefined && buffer.get.nonEmpty) {
      try {
        logger.info(s"刷新 $entityType 实体缓冲区，记录数: ${buffer.get.size}")

        // 直接写入到GraphAr格式
        writeEntityToGraphAr(buffer.get.toList, headerOpt.get, entityType)

        // 清空缓冲区
        buffer.get.clear()

      } catch {
        case e: Exception =>
          logger.error(s"刷新 $entityType 实体缓冲区时出错", e)
          buffer.get.clear() // 清空有问题的数据
      }
    }
  }

  /**
   * 将实体数据写入GraphAr格式
   */
  private def writeEntityToGraphAr(records: List[JList[String]], header: JList[String], entityType: String): Unit = {
    val headerList = header.asScala.toList
    logger.debug(s"写入 $entityType 实体到GraphAr，header: ${headerList.mkString(", ")}")

    // 创建从header到索引的映射
    val headerToIndexMap = headerList.zipWithIndex.toMap

    // 构建StructType（简化版本，使用String类型）
    val structFields = headerList.map { fieldName =>
      StructField(fieldName, org.apache.spark.sql.types.StringType, nullable = true)
    }
    val structType = StructType(structFields.toArray)

    // 转换数据记录
    import scala.collection.JavaConverters._
    val rows = records.map { record =>
      val recordArray = record.asScala.toArray
      val values = headerList.indices.map { index =>
        if (index < recordArray.length) recordArray(index) else null
      }
      Row.fromSeq(values)
    }

    // 创建DataFrame并写入
    val df = sparkSession.createDataFrame(rows.asJava, structType)
    val entityPath = getStandardGraphArPath(entityType)

    fileType.toLowerCase match {
      case "parquet" =>
        df.write.mode("append").parquet(entityPath)
      case "csv" =>
        df.write.mode("append").option("header", "true").option("encoding", "UTF-8").csv(entityPath)
      case "orc" =>
        df.write.mode("append").orc(entityPath)
      case _ =>
        logger.warn(s"不支持的文件类型: $fileType，使用parquet")
        df.write.mode("append").parquet(entityPath)
    }

    logger.info(s"成功写入 $entityType 实体 ${records.size} 条记录到 $entityPath")
  }

  // 实体数据缓冲区（按实体类型分组）
  private val entityBuffers = mutable.Map[String, mutable.ArrayBuffer[JList[String]]]()

  // 实体header映射
  private val entityHeaders = mutable.Map[String, Option[JList[String]]]()

  /**
   * 格式化时间戳
   */
  private def formatTimestamp(timestamp: Long): String = {
    val instant = java.time.Instant.ofEpochMilli(timestamp)
    instant.toString
  }

  /**
   * 写入数据记录（原有方法，保持兼容性）
   */
  override def write(record: JList[String]): Unit = {
    if (headerBuffer.isEmpty) {
      logger.warn("尚未接收到header，跳过数据记录")
      return
    }

    dataBuffer += record

    // 批量处理数据
    if (dataBuffer.size >= 1000) {
      processBatch()
    }
  }

  /**
   * 处理批量数据
   */
  private def processBatch(): Unit = {
    if (dataBuffer.isEmpty || headerBuffer.isEmpty || entityType.isEmpty || detectedSchema.isEmpty) {
      return
    }

    try {
      writeToGraphAr(dataBuffer.toList, headerBuffer.get, entityType.get, detectedSchema.get)
      batchCounter += 1
      logger.info(s"成功处理批次 $batchCounter，记录数: ${dataBuffer.size}")
      dataBuffer.clear()
    } catch {
      case e: Exception =>
        logger.error(s"处理批次数据时发生错误: $e", e)
        dataBuffer.clear()
    }
  }

  /**
   * 修正后的GraphAr写入方法 - 确保字段顺序匹配
   */
  private def writeToGraphAr(
                              records: List[JList[String]],
                              header: JList[String],
                              detectedEntityType: String,
                              schema: EntitySchemaDetector.EntitySchema
                            ): Unit = {

    val headerList = header.asScala.toList
    logger.info(s"原始header顺序: ${headerList.mkString(", ")}")

    // 获取属性分组字段（这些字段的顺序将决定StructType的字段顺序）
    val groupFields = propertyGroupManager.getGroupFields("basic").toList
    logger.info(s"属性分组字段顺序: ${groupFields.mkString(", ")}")

    // 创建从原始header到索引的映射
    val headerToIndexMap = headerList.zipWithIndex.toMap
    logger.info(s"Header索引映射: $headerToIndexMap")

    // 按照属性分组字段的顺序构建StructType
    val structFields = groupFields.map { fieldName =>
      val dataType = schema.getDataType(fieldName).getOrElse(org.apache.spark.sql.types.StringType)
      logger.info(s"字段映射: $fieldName -> $dataType")
      StructField(fieldName, dataType, nullable = true)
    }
    val structType = StructType(structFields.toArray)

    logger.info(s"最终StructType字段顺序: ${structType.fieldNames.mkString(", ")}")

    // 按照StructType字段顺序重新排列数据
    val rows = records.map { record =>
      val recordArray = record.asScala.toArray

      // 按照StructType字段顺序提取数据值
      val orderedValues = groupFields.map { fieldName =>
        headerToIndexMap.get(fieldName) match {
          case Some(index) if index < recordArray.length =>
            val rawValue = recordArray(index)
            val dataType = schema.getDataType(fieldName).getOrElse(org.apache.spark.sql.types.StringType)
            convertValue(rawValue, dataType)
          case Some(index) =>
            logger.warn(s"字段 $fieldName 的索引 $index 超出记录长度 ${recordArray.length}")
            null
          case None =>
            logger.warn(s"在header中未找到字段: $fieldName")
            null
        }
      }

      logger.debug(s"数据值顺序: ${orderedValues.mkString(", ")}")
      Row.fromSeq(orderedValues)
    }

    // 创建DataFrame并写入
    val df = sparkSession.createDataFrame(rows.asJava, structType)
    writeDataFrameToGraphAr(df, detectedEntityType)
  }

  /**
   * 数据类型转换
   */
  private def convertValue(value: String, dataType: org.apache.spark.sql.types.DataType): Any = {
    import org.apache.spark.sql.types._

    if (value == null || value.trim.isEmpty) return null

    try {
      dataType match {
        case LongType => value.toLong
        case IntegerType => value.toInt
        case StringType => value
        case BooleanType => value.toBoolean
        case TimestampType =>
          // 处理ISO 8601格式的时间戳
          if (value.contains("T") && value.contains("Z")) {
            java.sql.Timestamp.valueOf(value.replace("T", " ").replace("Z", ""))
          } else {
            java.sql.Timestamp.valueOf(value)
          }
        case DateType =>
          // 处理日期格式 YYYY-MM-DD
          if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
            java.sql.Date.valueOf(value)
          } else {
            null
          }
        case _ => value
      }
    } catch {
      case e: Exception =>
        logger.warn(s"转换值 '$value' 到类型 $dataType 失败: $e")
        null
    }
  }

  /**
   * 将DataFrame写入GraphAr格式
   */
  private def writeDataFrameToGraphAr(df: DataFrame, entityType: String): Unit = {
    logger.info(s"写入 $entityType 实体到GraphAr格式，记录数: ${df.count()}")

    val entityPath = getStandardGraphArPath(entityType)

    // 根据文件类型选择写入格式
    fileType.toLowerCase match {
      case "parquet" =>
        df.write
          .mode("append")
          .parquet(entityPath)

      case "csv" =>
        df.write
          .mode("append")
          .option("header", "true")
          .option("encoding", "UTF-8")
          .csv(entityPath)

      case "orc" =>
        df.write
          .mode("append")
          .orc(entityPath)

      case _ =>
        logger.warn(s"不支持的文件类型: $fileType，使用parquet")
        df.write
          .mode("append")
          .parquet(entityPath)
    }
  }

  /**
   * 获取统计信息
   */
  def getStatistics(): StreamingStatistics = {
    StreamingStatistics(
      recordCount = dataBuffer.size + batchCounter * 1000, // 估算总记录数
      processedCount = batchCounter * 1000,
      batchCount = batchCounter,
      entityType = entityType.getOrElse("Unknown"),
      isValidSchema = detectedSchema.isDefined && detectedSchema.get.isValidSchema
    )
  }

  /**
   * 关闭输出流，处理剩余数据
   */
  override def close(): Unit = {
    try {
      // 处理原有的数据缓冲区
      if (dataBuffer.nonEmpty) {
        processBatch()
      }

      // 刷新所有实体缓冲区
      entityBuffers.keys.foreach { entityType =>
        flushEntityBuffer(entityType)
      }

      logger.info(s"GraphArActivityOutputStream 已关闭")
      logger.info(s"处理的实体类型: ${entityBuffers.keys.mkString(", ")}")
      logger.info(s"总共处理 $batchCounter 个批次")

    } catch {
      case e: Exception =>
        logger.error(s"关闭输出流时发生错误: $e", e)
    }
  }
}

// 伴生对象
object GraphArActivityOutputStream {
  def apply(outputPath: String, graphName: String, fileType: String = "parquet")(implicit spark: SparkSession): GraphArActivityOutputStream = {
    new GraphArActivityOutputStream(outputPath, graphName, fileType)(spark)
  }
}

/**
 * 流式统计信息
 */
case class StreamingStatistics(
  recordCount: Long,
  processedCount: Long,
  batchCount: Long,
  entityType: String,
  isValidSchema: Boolean
)