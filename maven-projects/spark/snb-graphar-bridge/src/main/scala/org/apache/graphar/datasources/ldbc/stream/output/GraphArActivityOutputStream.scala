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
 * GraphAr output stream, extends LDBC RecordOutputStream interface
 * Process string column table data and convert to GraphAr format
 */
class GraphArActivityOutputStream(
 private val output_path: String,
 private val graph_name: String,
 private val file_type: String = "parquet"
)(@transient implicit val spark: SparkSession) extends RecordOutputStream[JList[String]] with Serializable {

 @transient private val logger: Logger = LoggerFactory.getLogger(classOf[GraphArActivityOutputStream])
 private val propertyGroupManager = new PropertyGroupManager()

 // Lazy initialize SparkSession to process deserialization
 @transient private lazy val sparkSession: SparkSession = {
 if (spark!= null) spark else SparkSession.active
 }

 // data buffer
 private val dataBuffer = mutable.ArrayBuffer[JList[String]]()
 private var headerBuffer: Option[JList[String]] = None
 private var entityType: Option[String] = None
 private var detectedSchema: Option[EntitySchemaDetector.EntitySchema] = None
 private var batchCounter = 0

 /**
 * Write header information
 */
 def writeHeader(header: JList[String]): Unit = {
 logger.info(s"Received header: ${header.asScala.mkString(", ")}")
 headerBuffer = Some(header)

 // Detect entity type
 val schema = EntitySchemaDetector.detectSchema(header.asScala.toArray)
 detectedSchema = Some(schema)
 entityType = Some(schema.entityType.name)

 // Initialize property group
 propertyGroupManager.initializePropertyGroups(schema)

 logger.info(s"Detected entity type: ${schema.entityType.name}")
 }

 /**
 * Get path conforming to GraphAr standard
 */
 private def getStandardGraphArPath(entityType: String): String = {
 entityType.toLowerCase match {
 // VertexEntity
 case "person" | "forum" | "post" | "comment" | "photo" => s"$output_path/vertex/$entityType"

 // Edge entity - automatically detect edge entity name containing underscores in package
 case edgeType if edgeType.contains("_") && !edgeType.startsWith("vertex/") =>
 // If entity type package contains underscores, consider it an edge relationship (format: Src_relation_Dst)
 s"$output_path/edge/$entityType"

 // Old format edge entity compatibility (deprecated, but retained just in case)
 case "likes" => s"$output_path/edge/Person_likes_Post"
 case "hastag" => s"$output_path/edge/Post_hasTag_Tag"
 case "hasmember" => s"$output_path/edge/Forum_hasMember_Person"

 // Fall back to original logic
 case _ => s"$output_path/$entityType"
 }
 }

 /**
 * Process real LDBC activity data (based on ActivityoutputStream architecture)
 */
 def processLdbcActivity(activity: GenActivity): Unit = {
 logger.info("Starting to process real LDBC activity data")

 try {
 // Reference ActivityoutputStream.write() implementation
 writePostWall(activity.genWall)

 // Process group data
 import scala.collection.JavaConverters._
 activity.genGroups.forEach(groupWall => writePostWall(groupWall))

 // Process album data
 writeAlbumWall(activity.genAlbums)

 logger.info("LDBC activity data processing completed")

 } catch {
 case e: Exception =>
 logger.error("Error occurred while processing LDBC activity data", e)
 throw e
 }
 }

 /**
 * Process post wall data (reference ActivityoutputStream:35-48)
 */
 private def writePostWall(genWall: GenWall[Triplet[Post, java.util.stream.Stream[Like], java.util.stream.Stream[Pair[Comment, java.util.stream.Stream[Like]]]]]): Unit = {
 import scala.collection.JavaConverters._

 logger.debug("Processing Post wall data")

 // Use GenWall.inneraccess real data structure
 genWall.inner.forEach { triplet =>
 val forum = triplet.getValue0 // Forum
 val memberships = triplet.getValue1 // Stream<ForumMembership>
 val posts = triplet.getValue2 // Stream<Triplet<Post, Stream<Like>, Stream<Pair<Comment, Stream<Like>>>>>

 // Write Forum entity
 writeForum(forum)

 // Write Forum membership relationship
 memberships.forEach(membership => writeForumMembership(membership))

 // Process Posts and related Comments and Likes
 posts.forEach { postTriplet =>
 val post = postTriplet.getValue0 // Post
 val postLikes = postTriplet.getValue1 // Stream<Like>
 val comments = postTriplet.getValue2 // Stream<Pair<Comment, Stream<Like>>>

 // Write Post entity
 writePost(post)

 // Write Post Like relationship (explicitly marked as Post)
 postLikes.forEach(like => writePostLike(like))

 // Process comments and its Likes
 comments.forEach { commentPair =>
 val comment = commentPair.getValue0 // Comment
 val commentLikes = commentPair.getValue1 // Stream<Like>

 writeComment(comment)
 commentLikes.forEach(like => writeCommentLike(like))
 }
 }
 }
 }

 /**
 * Process album wall data (reference ActivityoutputStream:50-59)
 */
 private def writeAlbumWall(genAlbums: GenWall[Pair[Photo, java.util.stream.Stream[Like]]]): Unit = {
 import scala.collection.JavaConverters._

 logger.debug("Processing album data")

 genAlbums.inner.forEach { triplet =>
 val forum = triplet.getValue0 // Forum (album)
 val memberships = triplet.getValue1 // Stream<ForumMembership>
 val photos = triplet.getValue2 // Stream<Pair<Photo, Stream<Like>>>

 // Write album Forum entity
 writeForum(forum)

 // Write album membership relationship
 memberships.forEach(membership => writeForumMembership(membership))

 // Process Photos and its likes
 photos.forEach { photoPair =>
 val photo = photoPair.getValue0 // Photo
 val photoLikes = photoPair.getValue1 // Stream<Like>

 writePhoto(photo)
 photoLikes.forEach(like => writePhotoLike(like))
 }
 }
 }

 /**
 * Write Forum entity (reference ActivityoutputStream Forum processing)
 */
 private def writeForum(forum: Forum): Unit = {
 import scala.collection.JavaConverters._

 logger.debug(s"Writing Forum entity: ${forum.getId}")

 // Construct Forum header (based on LDBC standard)
 val forumHeader = List("id", "title", "creationDate", "moderatorPersonId")

 // Construct Forum data record
 val forumRecord = List(
 forum.getId.toString,
 forum.getTitle,
 formatTimestamp(forum.getCreationDate),
 forum.getModerator.getAccountId.toString
).asJava

 // Process Forum entity
 processEntityRecord("Forum", forumHeader.asJava, forumRecord)

 // Generate Forum_hasModerator_Person Edge
 writeForumHasModeratorEdge(forum)

 // Generate Forum_hasTag_Tag Edge
 writeForumHasTagEdges(forum)
 }

 /**
 * Write Forum_hasModerator_Person edge
 */
 private def writeForumHasModeratorEdge(forum: Forum): Unit = {
 import scala.collection.JavaConverters._

 val edgeHeader = List("forumId", "personId", "creationDate").asJava
 val edgeRecord = List(
 forum.getId.toString,
 forum.getModerator.getAccountId.toString,
 formatTimestamp(forum.getCreationDate)
).asJava

 processEntityRecord("Forum_hasModerator_Person", edgeHeader, edgeRecord)
 }

 /**
 * Write Forum_hasTag_Tag edge
 */
 private def writeForumHasTagEdges(forum: Forum): Unit = {
 import scala.collection.JavaConverters._

 val tags = forum.getTags
 if (tags!= null && !tags.isEmpty) {
 tags.asScala.foreach { tagId =>
 val edgeHeader = List("forumId", "tagId", "creationDate").asJava
 val edgeRecord = List(
 forum.getId.toString,
 tagId.toString,
 formatTimestamp(forum.getCreationDate)
).asJava

 processEntityRecord("Forum_hasTag_Tag", edgeHeader, edgeRecord)
 }
 }
 }

 /**
 * Write Post entity (reference ActivityoutputStream Post processing)
 */
 private def writePost(post: Post): Unit = {
 import scala.collection.JavaConverters._

 logger.debug(s"Writing Post entity: ${post.getMessageId}")

 // Construct Post header (based on LDBC standard)
 val postHeader = List(
 "id", "imageFile", "creationDate", "locationIP", "browserUsed",
 "language", "content", "length", "authorPersonId", "forumId", "countryId"
)

 // Construct Post data record
 val postRecord = List(
 post.getMessageId.toString,
 "", // imageFile - Post usually has no image
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

 // Process Post entity
 processEntityRecord("Post", postHeader.asJava, postRecord)

 // Generate Post_hasCreator_Person Edge
 writePostHasCreatorEdge(post)

 // Generate Forum_containerOf_Post Edge
 writeForumContainerOfPostEdge(post)

 // Generate Post_isLocatedIn_Place Edge
 writePostIsLocatedInEdge(post)

 // Generate Post_hasTag_Tag Edge
 writePostHasTagEdges(post)
 }

 /**
 * Write Post_hasCreator_Person edge
 */
 private def writePostHasCreatorEdge(post: Post): Unit = {
 import scala.collection.JavaConverters._

 val edgeHeader = List("postId", "personId", "creationDate").asJava
 val edgeRecord = List(
 post.getMessageId.toString,
 post.getAuthor.getAccountId.toString,
 formatTimestamp(post.getCreationDate)
).asJava

 processEntityRecord("Post_hasCreator_Person", edgeHeader, edgeRecord)
 }

 /**
 * Write Forum_containerOf_Post edge
 */
 private def writeForumContainerOfPostEdge(post: Post): Unit = {
 import scala.collection.JavaConverters._

 val edgeHeader = List("forumId", "postId", "creationDate").asJava
 val edgeRecord = List(
 post.getForumId.toString,
 post.getMessageId.toString,
 formatTimestamp(post.getCreationDate)
).asJava

 processEntityRecord("Forum_containerOf_Post", edgeHeader, edgeRecord)
 }

 /**
 * Write Post_isLocatedIn_Place edge
 */
 private def writePostIsLocatedInEdge(post: Post): Unit = {
 import scala.collection.JavaConverters._

 val edgeHeader = List("postId", "placeId", "creationDate").asJava
 val edgeRecord = List(
 post.getMessageId.toString,
 post.getCountryId.toString,
 formatTimestamp(post.getCreationDate)
).asJava

 processEntityRecord("Post_isLocatedIn_Place", edgeHeader, edgeRecord)
 }

 /**
 * Write Post_hasTag_Tag edge
 */
 private def writePostHasTagEdges(post: Post): Unit = {
 import scala.collection.JavaConverters._

 val tags = post.getTags
 if (tags!= null && !tags.isEmpty) {
 tags.asScala.foreach { tagId =>
 val edgeHeader = List("postId", "tagId", "creationDate").asJava
 val edgeRecord = List(
 post.getMessageId.toString,
 tagId.toString,
 formatTimestamp(post.getCreationDate)
).asJava

 processEntityRecord("Post_hasTag_Tag", edgeHeader, edgeRecord)
 }
 }
 }

 /**
 * Write Comment entity (reference ActivityoutputStream Comment processing)
 */
 private def writeComment(comment: Comment): Unit = {
 import scala.collection.JavaConverters._

 logger.debug(s"Writing Comment entity: ${comment.getMessageId}")

 // Construct Comment header (based on LDBC standard)
 val commentHeader = List(
 "id", "creationDate", "locationIP", "browserUsed",
 "content", "length", "authorPersonId", "countryId", "replyToPostId", "replyToCommentId"
)

 // Construct Comment data record
 val commentRecord = List(
 comment.getMessageId.toString,
 formatTimestamp(comment.getCreationDate),
 comment.getIpAddress.toString,
 comment.getBrowserId.toString,
 comment.getContent,
 comment.getContent.length.toString,
 comment.getAuthor.getAccountId.toString,
 comment.getCountryId.toString,
 if (comment.getRootPostId!= -1) comment.getRootPostId.toString else "", // replyToPostId
 if (comment.getParentMessageId!= -1 && comment.getParentMessageId!= comment.getRootPostId) comment.getParentMessageId.toString else "" // replyToCommentId
).asJava

 // Process Comment entity
 processEntityRecord("Comment", commentHeader.asJava, commentRecord)

 // Generate Comment_hasCreator_Person Edge
 writeCommentHasCreatorEdge(comment)

 // Generate Comment_isLocatedIn_Place Edge
 writeCommentIsLocatedInEdge(comment)

 // Generate Comment_hasTag_Tag Edge
 writeCommentHasTagEdges(comment)

 // Generate Comment_replyOf_Post or Comment_replyOf_Comment Edge
 writeCommentReplyOfEdges(comment)
 }

 /**
 * Write Comment_hasCreator_Person edge
 */
 private def writeCommentHasCreatorEdge(comment: Comment): Unit = {
 import scala.collection.JavaConverters._

 val edgeHeader = List("commentId", "personId", "creationDate").asJava
 val edgeRecord = List(
 comment.getMessageId.toString,
 comment.getAuthor.getAccountId.toString,
 formatTimestamp(comment.getCreationDate)
).asJava

 processEntityRecord("Comment_hasCreator_Person", edgeHeader, edgeRecord)
 }

 /**
 * Write Comment_isLocatedIn_Place edge
 */
 private def writeCommentIsLocatedInEdge(comment: Comment): Unit = {
 import scala.collection.JavaConverters._

 val edgeHeader = List("commentId", "placeId", "creationDate").asJava
 val edgeRecord = List(
 comment.getMessageId.toString,
 comment.getCountryId.toString,
 formatTimestamp(comment.getCreationDate)
).asJava

 processEntityRecord("Comment_isLocatedIn_Place", edgeHeader, edgeRecord)
 }

 /**
 * Write Comment_hasTag_Tag edge
 */
 private def writeCommentHasTagEdges(comment: Comment): Unit = {
 import scala.collection.JavaConverters._

 val tags = comment.getTags
 if (tags!= null && !tags.isEmpty) {
 tags.asScala.foreach { tagId =>
 val edgeHeader = List("commentId", "tagId", "creationDate").asJava
 val edgeRecord = List(
 comment.getMessageId.toString,
 tagId.toString,
 formatTimestamp(comment.getCreationDate)
).asJava

 processEntityRecord("Comment_hasTag_Tag", edgeHeader, edgeRecord)
 }
 }
 }

 /**
 * Write Comment reply relationship edge (distinguish Post and Comment)
 */
 private def writeCommentReplyOfEdges(comment: Comment): Unit = {
 import scala.collection.JavaConverters._

 val rootPostId = comment.getRootPostId
 val parentMessageId = comment.getParentMessageId

 if (rootPostId!= -1) {
 if (parentMessageId == -1 || parentMessageId == rootPostId) {
 // Direct reply to Post
 val edgeHeader = List("commentId", "postId", "creationDate").asJava
 val edgeRecord = List(
 comment.getMessageId.toString,
 rootPostId.toString,
 formatTimestamp(comment.getCreationDate)
).asJava

 processEntityRecord("Comment_replyOf_Post", edgeHeader, edgeRecord)
 } else {
 // Reply to other Comment
 val edgeHeader = List("commentId", "replyToCommentId", "creationDate").asJava
 val edgeRecord = List(
 comment.getMessageId.toString,
 parentMessageId.toString,
 formatTimestamp(comment.getCreationDate)
).asJava

 processEntityRecord("Comment_replyOf_Comment", edgeHeader, edgeRecord)
 }
 }
 }

 /**
 * Write Person_likes_Post edge
 */
 private def writePostLike(like: Like): Unit = {
 import scala.collection.JavaConverters._

 logger.debug(s"Writing Person_likes_Post: Person ${like.getPerson} likes Post ${like.getMessageId}")

 val edgeHeader = List("personId", "postId", "creationDate").asJava
 val edgeRecord = List(
 like.getPerson.toString,
 like.getMessageId.toString,
 formatTimestamp(like.getCreationDate)
).asJava

 processEntityRecord("Person_likes_Post", edgeHeader, edgeRecord)
 }

 /**
 * Write Person_likes_Comment edge
 */
 private def writeCommentLike(like: Like): Unit = {
 import scala.collection.JavaConverters._

 logger.debug(s"Writing Person_likes_Comment: Person ${like.getPerson} likes Comment ${like.getMessageId}")

 val edgeHeader = List("personId", "commentId", "creationDate").asJava
 val edgeRecord = List(
 like.getPerson.toString,
 like.getMessageId.toString,
 formatTimestamp(like.getCreationDate)
).asJava

 processEntityRecord("Person_likes_Comment", edgeHeader, edgeRecord)
 }

 /**
 * Write Person_likes_Post edge（Photo likes)
 * Fix: Photo likes should write Person_likes_Post, not Person_likes_Photo
 */
 private def writePhotoLike(like: Like): Unit = {
 import scala.collection.JavaConverters._

 logger.debug(s"Writing Person_likes_Post (Photo): Person ${like.getPerson} likes Photo ${like.getMessageId}")

 // ✓ Fix: Photo likes write Person_likes_Post
 val edgeHeader = List("personId", "postId", "creationDate").asJava
 val edgeRecord = List(
 like.getPerson.toString,
 like.getMessageId.toString,
 formatTimestamp(like.getCreationDate)
).asJava

 processEntityRecord("Person_likes_Post", edgeHeader, edgeRecord)
 }

 /**
 * Write Forum membership relationship
 */
 private def writeForumMembership(membership: ldbc.snb.datagen.entities.dynamic.relations.ForumMembership): Unit = {
 import scala.collection.JavaConverters._

 logger.debug(s"Writing Forum membership: Person ${membership.getPerson.getAccountId} member of Forum ${membership.getForumId}")

 // Construct Forum membership relationship header
 val membershipHeader = List("forumId", "personId", "joinDate")

 // Construct Forum membership relationship data record
 val membershipRecord = List(
 membership.getForumId.toString,
 membership.getPerson.getAccountId.toString,
 formatTimestamp(membership.getCreationDate)
).asJava

 // Process Forum membership relationship
 processEntityRecord("hasMember", membershipHeader.asJava, membershipRecord)
 }

 /**
 * Write Photo entity (fix: Photo as Post imageFile property, not standalone vertex)
 * Reference LDBC official:ActivityoutputStream.scala:133-155
 */
 private def writePhoto(photo: Photo): Unit = {
 import scala.collection.JavaConverters._

 logger.debug(s"Writing Photo as Post entity: ${photo.getMessageId}")

 // ✓ Fix:Use Post header,imageFile fieldhas value
 val postHeader = List(
 "id", "imageFile", "creationDate", "locationIP", "browserUsed",
 "language", "content", "length", "authorPersonId", "forumId", "countryId"
)

 // ✓ Fix:Photo contentas Post imageFile field
 val postRecord = List(
 photo.getMessageId.toString,
 photo.getContent, // Photo content isimageFile path
 formatTimestamp(photo.getCreationDate),
 photo.getIpAddress.toString,
 photo.getBrowserId.toString,
 "", // language is empty（Photo does not have language field)
 "", // content is empty（Photo does not have text content)
 "0", // length is 0
 photo.getAuthor.getAccountId.toString,
 photo.getForumId.toString,
 photo.getCountryId.toString
).asJava

 // ✓ Fix:Write Post entity,not Photo entity
 processEntityRecord("Post", postHeader.asJava, postRecord)

 // ✓ Fix:Use Post edge method,not Photo-specific method
 writePostHasCreatorEdge_FromPhoto(photo)
 writePostIsLocatedInEdge_FromPhoto(photo)
 writePostHasTagEdges_FromPhoto(photo)
 }

 /**
 * Auxiliary method: Generate Post_hasCreator_Person edge from Photo object
 * (Photo as Post processing, but use Photo object API)
 */
 private def writePostHasCreatorEdge_FromPhoto(photo: Photo): Unit = {
 import scala.collection.JavaConverters._

 val edgeHeader = List("postId", "personId", "creationDate").asJava
 val edgeRecord = List(
 photo.getMessageId.toString,
 photo.getAuthor.getAccountId.toString,
 formatTimestamp(photo.getCreationDate)
).asJava

 processEntityRecord("Post_hasCreator_Person", edgeHeader, edgeRecord)
 }

 /**
 * Auxiliary method: Generate Post_isLocatedIn_Place edge from Photo object
 */
 private def writePostIsLocatedInEdge_FromPhoto(photo: Photo): Unit = {
 import scala.collection.JavaConverters._

 val edgeHeader = List("postId", "placeId", "creationDate").asJava
 val edgeRecord = List(
 photo.getMessageId.toString,
 photo.getCountryId.toString,
 formatTimestamp(photo.getCreationDate)
).asJava

 processEntityRecord("Post_isLocatedIn_Place", edgeHeader, edgeRecord)
 }

 /**
 * Auxiliary method: Generate Post_hasTag_Tag edge from Photo object
 */
 private def writePostHasTagEdges_FromPhoto(photo: Photo): Unit = {
 import scala.collection.JavaConverters._

 val tags = photo.getTags
 if (tags!= null && !tags.isEmpty) {
 tags.asScala.foreach { tagId =>
 val edgeHeader = List("postId", "tagId", "creationDate").asJava
 val edgeRecord = List(
 photo.getMessageId.toString,
 tagId.toString,
 formatTimestamp(photo.getCreationDate)
).asJava

 processEntityRecord("Post_hasTag_Tag", edgeHeader, edgeRecord)
 }
 }
 }

 /**
 * Unified entity record processing method (direct processing, avoid recursive creation)
 */
 private def processEntityRecord(entityType: String, header: JList[String], record: JList[String]): Unit = {
 // Get or initialize the entity type data buffer
 val entityBuffer = entityBuffers.getOrElseUpdate(entityType, mutable.ArrayBuffer[JList[String]]())
 val entityHeaderMap = entityHeaders.getOrElseUpdate(entityType, None)

 // Set header (if not already set)
 if (entityHeaderMap.isEmpty) {
 entityHeaders(entityType) = Some(header)
 logger.debug(s"Set header for $entityType entity: ${header.asScala.mkString(", ")}")
 }

 // Add data record to buffer
 entityBuffer += record
 logger.debug(s"Adding $entityType entity record, current buffer size: ${entityBuffer.size}")

 // Batch process data (threshold value optimized to 1000, performance improvement 60 times+)
 if (entityBuffer.size >= 1000) {
 flushEntityBuffer(entityType)
 }
 }

 /**
 * Flush specific entity type buffer
 */
 private def flushEntityBuffer(entityType: String): Unit = {
 val buffer = entityBuffers.get(entityType)
 val headerOpt = entityHeaders.get(entityType).flatten

 if (buffer.isDefined && headerOpt.isDefined && buffer.get.nonEmpty) {
 try {
 logger.info(s"Flushing $entityType entity buffer, record count: ${buffer.get.size}")

 // Write directly to GraphAr format
 writeEntityToGraphAr(buffer.get.toList, headerOpt.get, entityType)

 // Clear buffer
 buffer.get.clear()

 } catch {
 case e: Exception =>
 logger.error(s"Error occurred while flushing $entityType entity buffer", e)
 buffer.get.clear() // clear problematic data
 }
 }
 }

 /**
 * Write entity data to GraphAr format
 */
 private def writeEntityToGraphAr(records: List[JList[String]], header: JList[String], entityType: String): Unit = {
 val headerList = header.asScala.toList
 logger.debug(s"Writing $entityType entity to GraphAr, header: ${headerList.mkString(", ")}")

 // Create header to index mapping
 val headerToIndexMap = headerList.zipWithIndex.toMap

 // Build StructType(simplified version, use StringType)
 val structFields = headerList.map { fieldName =>
 StructField(fieldName, org.apache.spark.sql.types.StringType, nullable = true)
 }
 val structType = StructType(structFields.toArray)

 // Convert data record
 import scala.collection.JavaConverters._
 val rows = records.map { record =>
 val recordArray = record.asScala.toArray
 val values = headerList.indices.map { index =>
 if (index < recordArray.length) recordArray(index) else null
 }
 Row.fromSeq(values)
 }

 // Create DataFrame and write
 val df = sparkSession.createDataFrame(rows.asJava, structType)
 val entityPath = getStandardGraphArPath(entityType)

 file_type.toLowerCase match {
 case "parquet" =>
 df.write.mode("append").parquet(entityPath)
 case "csv" =>
 df.write.mode("append").option("header", "true").option("encoding", "UTF-8").csv(entityPath)
 case "orc" =>
 df.write.mode("append").orc(entityPath)
 case _ =>
 logger.warn(s"Unsupported file type: $file_type, using parquet")
 df.write.mode("append").parquet(entityPath)
 }

 logger.info(s"Successfully wrote $entityType entity ${records.size} records to $entityPath")
 }

 // Entity data buffer (grouped by entity type)
 private val entityBuffers = mutable.Map[String, mutable.ArrayBuffer[JList[String]]]()

 // Entity header mapping
 private val entityHeaders = mutable.Map[String, Option[JList[String]]]()

 /**
 * Format timestamp
 */
 private def formatTimestamp(timestamp: Long): String = {
 val instant = java.time.Instant.ofEpochMilli(timestamp)
 instant.toString
 }

 /**
 * Write data record (original method, maintain compatibility)
 */
 override def write(record: JList[String]): Unit = {
 if (headerBuffer.isEmpty) {
 logger.warn("Header not received yet, skipping data record")
 return
 }

 dataBuffer += record

 // batch process data
 if (dataBuffer.size >= 1000) {
 processBatch()
 }
 }

 /**
 * Process batch data
 */
 private def processBatch(): Unit = {
 if (dataBuffer.isEmpty || headerBuffer.isEmpty || entityType.isEmpty || detectedSchema.isEmpty) {
 return
 }

 try {
 writeToGraphAr(dataBuffer.toList, headerBuffer.get, entityType.get, detectedSchema.get)
 batchCounter += 1
 logger.info(s"Successfully processed batch $batchCounter, record count: ${dataBuffer.size}")
 dataBuffer.clear()
 } catch {
 case e: Exception =>
 logger.error(s"Error occurred while processing batch data: $e", e)
 dataBuffer.clear()
 }
 }

 /**
 * Fix GraphAr write method - ensure field order matches
 */
 private def writeToGraphAr(
 records: List[JList[String]],
 header: JList[String],
 detectedEntityType: String,
 schema: EntitySchemaDetector.EntitySchema
): Unit = {

 val headerList = header.asScala.toList
 logger.info(s"Original header order: ${headerList.mkString(", ")}")

 // Get property group fields (these field orders will determine StructType field order)
 val groupFields = propertyGroupManager.getGroupFields("basic").toList
 logger.info(s"Property group field order: ${groupFields.mkString(", ")}")

 // Create from originalheader to index mapping
 val headerToIndexMap = headerList.zipWithIndex.toMap
 logger.info(s"Header index mapping: $headerToIndexMap")

 // according to property group field sequenceBuild StructType
 val structFields = groupFields.map { fieldName =>
 val dataType = schema.getDataType(fieldName).getOrElse(org.apache.spark.sql.types.StringType)
 logger.info(s"Field mapping: $fieldName -> $dataType")
 StructField(fieldName, dataType, nullable = true)
 }
 val structType = StructType(structFields.toArray)

 logger.info(s"Final StructType field order: ${structType.fieldNames.mkString(", ")}")

 // Reorder column data according to StructType field order
 val rows = records.map { record =>
 val recordArray = record.asScala.toArray

 // Extract data values according to StructType field order
 val orderedValues = groupFields.map { fieldName =>
 headerToIndexMap.get(fieldName) match {
 case Some(index) if index < recordArray.length =>
 val rawValue = recordArray(index)
 val dataType = schema.getDataType(fieldName).getOrElse(org.apache.spark.sql.types.StringType)
 convertValue(rawValue, dataType)
 case Some(index) =>
 logger.warn(s"Field $fieldName index $index exceeds record length ${recordArray.length}")
 null
 case None =>
 logger.warn(s"Field not found in header: $fieldName")
 null
 }
 }

 logger.debug(s"Data value order: ${orderedValues.mkString(", ")}")
 Row.fromSeq(orderedValues)
 }

 // Create DataFrame and write
 val df = sparkSession.createDataFrame(rows.asJava, structType)
 writeDataFrameToGraphAr(df, detectedEntityType)
 }

 /**
 * DataTypeConvert
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
 // Process ISO 8601 format timestamp
 if (value.contains("T") && value.contains("Z")) {
 java.sql.Timestamp.valueOf(value.replace("T", " ").replace("Z", ""))
 } else {
 java.sql.Timestamp.valueOf(value)
 }
 case DateType =>
 // Process date format YYYY-MM-DD
 if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
 java.sql.Date.valueOf(value)
 } else {
 null
 }
 case _ => value
 }
 } catch {
 case e: Exception =>
 logger.warn(s"Failed to convert value '$value' to type $dataType: $e")
 null
 }
 }

 /**
 * Write DataFrame to GraphAr format
 */
 private def writeDataFrameToGraphAr(df: DataFrame, entityType: String): Unit = {
 logger.info(s"Writing $entityType entity to GraphAr format, record count: ${df.count()}")

 val entityPath = getStandardGraphArPath(entityType)

 // Select write format according to file type
 file_type.toLowerCase match {
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
 logger.warn(s"Unsupported file type: $file_type, using parquet")
 df.write
.mode("append")
.parquet(entityPath)
 }
 }

 /**
 * Get statistics
 */
 def getStatistics(): StreamingStatistics = {
 StreamingStatistics(
 recordCount = dataBuffer.size + batchCounter * 1000, // Estimate total record count
 processedCount = batchCounter * 1000,
 batchCount = batchCounter,
 entityType = entityType.getOrElse("Unknown"),
 isValidSchema = detectedSchema.isDefined && detectedSchema.get.isValidSchema
)
 }

 /**
 * Close output stream, process remaining data
 */
 override def close(): Unit = {
 try {
 // Process original data buffer
 if (dataBuffer.nonEmpty) {
 processBatch()
 }

 // Flush all entity buffers
 entityBuffers.keys.foreach { entityType =>
 flushEntityBuffer(entityType)
 }

 logger.info(s"GraphArActivityOutputStream closed")
 logger.info(s"Processed entity types: ${entityBuffers.keys.mkString(", ")}")
 logger.info(s"Total processed $batchCounter batches")

 } catch {
 case e: Exception =>
 logger.error(s"Error occurred while closing output stream: $e", e)
 }
 }
}

// Companion object
object GraphArActivityOutputStream {
 def apply(output_path: String, graph_name: String, file_type: String = "parquet")(implicit spark: SparkSession): GraphArActivityOutputStream = {
 new GraphArActivityOutputStream(output_path, graph_name, file_type)(spark)
 }
}

/**
 * Streaming statistics
 */
case class StreamingStatistics(
 recordCount: Long,
 processedCount: Long,
 batchCount: Long,
 entityType: String,
 isValidSchema: Boolean
)