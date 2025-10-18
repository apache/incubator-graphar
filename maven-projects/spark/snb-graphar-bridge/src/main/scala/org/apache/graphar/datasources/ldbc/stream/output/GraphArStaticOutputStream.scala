/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.graphar.datasources.ldbc.stream.output

import ldbc.snb.datagen.generator.dictionary.Dictionaries
import ldbc.snb.datagen.generator.serializers.StaticGraph
import ldbc.snb.datagen.generator.vocabulary.{DBP, DBPOWL}
import ldbc.snb.datagen.entities.statictype.Organisation
import ldbc.snb.datagen.entities.statictype.place.Place
import ldbc.snb.datagen.io.raw.RecordOutputStream
import ldbc.snb.datagen.util.StringUtils
import org.apache.graphar.datasources.ldbc.model._
import org.apache.graphar.datasources.ldbc.stream.processor.ContinuousIdGenerator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

/**
 * GraphArStatic entityoutput stream
 *
 * Intercept LDBC RecordOutputStream[StaticGraph.type], convert static entity data to GraphAr format.
 * This class implements architecture symmetry with dynamic entity processing, reusing streaming interception mechanism to process static data.
 *
 * Support Static entity:
 * - Place: Geographic location entity (countries, cities, etc)
 * - Tag:LabelEntity
 * - TagClass: Tag classification entity
 * - Organisation: Organization entity (companies, universities)
 *
 * Support Staticrelationship:
 * - Place-isPartOf-Place: Geographic hierarchical relationship
 * - Tag-hasType-TagClass: Tag classification relationship
 * - TagClass-isSubclassOf-TagClass: Classification hierarchical relationship
 * - Organisation-isLocatedIn-Place: Organization location relationship
 */
class GraphArStaticOutputStream(
 output_path: String,
 graph_name: String,
 vertex_chunk_size: Long = 1024L,
 edge_chunk_size: Long = 1024L,
 file_type: String = "parquet"
)(@transient implicit val spark: SparkSession) extends RecordOutputStream[StaticGraph.type] with Serializable {

 @transient private val logger: Logger = LoggerFactory.getLogger(classOf[GraphArStaticOutputStream])

 // Lazy initialize SparkSession to process deserialization
 @transient private lazy val sparkSession: SparkSession = {
 if (spark!= null) spark else SparkSession.active
 }

 // Continuous ID generator, ensure GraphAr format requires continuous IDs
 private val placeIdGenerator = new ContinuousIdGenerator()
 private val tagIdGenerator = new ContinuousIdGenerator()
 private val tagClassIdGenerator = new ContinuousIdGenerator()
 private val organisationIdGenerator = new ContinuousIdGenerator()

 // Original ID to continuous ID mapping table, used to process relationships between entities
 private val placeIdmapping = mutable.Map[Long, Long]()
 private val tagIdmapping = mutable.Map[Int, Long]() // Tag IDisIntegerType
 private val tagClassIdmapping = mutable.Map[Int, Long]()
 private val organisationIdmapping = mutable.Map[Int, Long]()

 // Already exported TagClass set, avoid duplicate export
 private val exportedTagClasses = mutable.Set[Int]()

 // DataCache,used tobatchWriteGraphAr
 private val placedataBuffer = mutable.ListBuffer[PlaceData]()
 private val tagdataBuffer = mutable.ListBuffer[TagData]()
 private val tagClassdataBuffer = mutable.ListBuffer[TagClassData]()
 private val organisationdataBuffer = mutable.ListBuffer[OrganisationData]()

 // relationshipDataCache
 private val placeRelationBuffer = mutable.ListBuffer[PlaceIsPartOfPlaceData]()
 private val tagRelationBuffer = mutable.ListBuffer[TagHasTypeTagClassData]()
 private val tagClassRelationBuffer = mutable.ListBuffer[TagClassIsSubclassOfTagClassData]()
 private val organisationRelationBuffer = mutable.ListBuffer[OrganisationIsLocatedInPlaceData]()

 logger.info(s"GraphArStaticOutputStream initialized: output_path=$output_path, file_type=$file_type")

 /**
 * Core method for intercepting static entity data write
 *
 * When LDBC generator calls StaticOutputStream.write(StaticGraph),
 * This method will be triggered, we intercept and convert data here
 */
 override def write(staticGraph: StaticGraph.type): Unit = {
 logger.info("=== Starting to intercept static entity data and convert to GraphAr format ===")

 try {
 // Process each class of static entities according to StaticOutputStream.scala logic
 processPlaceEntities() // Place + Place hierarchical relationships
 processTagEntities() // Tag + TagClass hierarchical relationships
 processOrganisationEntities() // Company + University
 generateStaticRelations() // Relationships between static entities

 // batchwrite GraphAr format
 writeStaticEntitiesToGraphAr()

 logger.info("✓ Static entity GraphAr conversion completed")

 } catch {
 case e: Exception =>
 logger.error("Error occurred during static entity conversion", e)
 throw e
 }
 }

 /**
 * ProcessPlaceEntity
 * Reuse StaticOutputStream.scala:61-67 logic
 */
 private def processPlaceEntities(): Unit = {
 logger.info("Starting to process Place entities...")

 // VerifyDictionaries.placeswhetheralreadyInitialize
 if (Dictionaries.places == null) {
 logger.error("Dictionaries.places not initialized, cannot process Place entities")
 return
 }

 val places = Dictionaries.places.getPlaces
 if (places == null || places.isEmpty) {
 logger.warn("Places dictionary is empty, skipping Place entity processing")
 return
 }

 logger.info(s"Places dictionary contains ${places.size()} places")
 val locations = places.iterator().asScala

 for { location <- locations } {
 val place = Dictionaries.places.getLocation(location)
 if (place!= null) {
 place.setName(StringUtils.clampString(place.getName, 256))

 val placeId = placeIdGenerator.getNextId("Place", place.getId)
 placeIdmapping(place.getId) = placeId

 val partOfPlaceId = place.getType match {
 case Place.CITY | Place.COUNTRY =>
 val parentPlaceId = Dictionaries.places.belongsTo(place.getId)
 if (parentPlaceId!= -1) Some(parentPlaceId) else None
 case _ => None
 }

 val placeData = PlaceData(
 id = placeId,
 originalId = place.getId,
 name = place.getName,
 url = DBP.getUrl(place.getName),
 placeType = place.getType, // Place.getTypeReturnStringType
 partOfPlaceId = partOfPlaceId.map(_.toLong) // ConvertisLongType
)

 placedataBuffer += placeData

 // Process Place hierarchical relationships
 partOfPlaceId.foreach { parentId =>
 // Ensure parent Place has already been processed
 if (placeIdmapping.contains(parentId)) {
 placeRelationBuffer += PlaceIsPartOfPlaceData(
 src = placeId,
 dst = placeIdmapping(parentId)
)
 }
 }
 }
 }

 logger.info(s"✓ Successfully processed ${placedataBuffer.size} Place entities")
 }

 /**
 * Process Tag entities and TagClass hierarchical structure
 * Reuse StaticOutputStream.scala:69-83 logic
 */
 private def processTagEntities(): Unit = {
 logger.info("Starting to process Tag entities...")

 val tags = Dictionaries.tags.getTags.iterator().asScala

 for { tag <- tags } {
 val tagName = StringUtils
.clampString(Dictionaries.tags.getName(tag), 256)
.replace("\"", "\\\"")

 val tagId = tagIdGenerator.getNextId("Tag", tag.toLong)
 tagIdmapping(tag) = tagId

 val tagData = TagData(
 id = tagId,
 originalId = tag.toLong, // ConvertisLongType
 name = tagName,
 url = DBP.getUrl(tagName),
 typeTagClassId = Dictionaries.tags.getTagClass(tag)
)

 tagdataBuffer += tagData

 // Process TagClass hierarchical structure
 processTagClassHierarchy(tagData)

 // ProcessTag-TagClassrelationship
 val tagClassId = Dictionaries.tags.getTagClass(tag)
 if (tagClassIdmapping.contains(tagClassId)) {
 tagRelationBuffer += TagHasTypeTagClassData(
 src = tagId,
 dst = tagClassIdmapping(tagClassId)
)
 }
 }

 logger.info(s"Processed ${tagdataBuffer.size} Tag entities")
 }

 /**
 * Process TagClass hierarchical structure
 * Reuse StaticOutputStream.scala:43-58 logic
 */
 private def processTagClassHierarchy(tag: TagData): Unit = {
 var classId = tag.typeTagClassId

 while (classId!= -1 && !exportedTagClasses.contains(classId)) {
 exportedTagClasses.add(classId)

 val classParent = Dictionaries.tags.getClassParent(classId)
 val className = StringUtils.clampString(Dictionaries.tags.getClassName(classId), 256)

 val tagClassId = tagClassIdGenerator.getNextId("TagClass", classId)
 tagClassIdmapping(classId) = tagClassId

 val tagClassData = TagClassData(
 id = tagClassId,
 originalId = classId,
 name = className,
 url = if (className == "Thing") "http://www.w3.org/2002/07/owl#Thing" else DBPOWL.getUrl(className),
 subclassOfId = if (classParent!= -1) Some(classParent) else None
)

 tagClassdataBuffer += tagClassData

 // Process TagClass hierarchical relationships
 if (classParent!= -1 && tagClassIdmapping.contains(classParent)) {
 tagClassRelationBuffer += TagClassIsSubclassOfTagClassData(
 src = tagClassId,
 dst = tagClassIdmapping(classParent)
)
 }

 classId = classParent
 }
 }

 /**
 * ProcessOrganisationEntity（Company + University)
 * Reuse StaticOutputStream.scala:85-112 logic
 */
 private def processOrganisationEntities(): Unit = {
 logger.info("Starting to process Organisation entities...")

 // Process company data
 val companies = Dictionaries.companies.getCompanies.iterator().asScala
 for { company <- companies } {
 val companyName = StringUtils.clampString(Dictionaries.companies.getCompanyName(company), 256)
 val organisationId = organisationIdGenerator.getNextId("Organisation", company.toInt)
 organisationIdmapping(company.toInt) = organisationId

 val organisationData = OrganisationData(
 id = organisationId,
 originalId = company.toInt,
 organisationType = Organisation.OrganisationType.Company.toString,
 name = companyName,
 url = DBP.getUrl(companyName),
 locationId = Dictionaries.companies.getCountry(company)
)

 organisationdataBuffer += organisationData

 // ProcessOrganisation-Placerelationship
 val countryId = Dictionaries.companies.getCountry(company)
 if (placeIdmapping.contains(countryId.toLong)) {
 organisationRelationBuffer += OrganisationIsLocatedInPlaceData(
 src = organisationId,
 dst = placeIdmapping(countryId.toLong)
)
 }
 }

 // Process university data
 val universities = Dictionaries.universities.getUniversities.iterator().asScala
 for { university <- universities } {
 val universityName = StringUtils.clampString(Dictionaries.universities.getUniversityName(university), 256)
 val organisationId = organisationIdGenerator.getNextId("Organisation", university.toInt)
 organisationIdmapping(university.toInt) = organisationId

 val organisationData = OrganisationData(
 id = organisationId,
 originalId = university.toInt,
 organisationType = Organisation.OrganisationType.University.toString,
 name = universityName,
 url = DBP.getUrl(universityName),
 locationId = Dictionaries.universities.getUniversityCity(university)
)

 organisationdataBuffer += organisationData

 // ProcessOrganisation-Placerelationship
 val cityId = Dictionaries.universities.getUniversityCity(university)
 if (placeIdmapping.contains(cityId.toLong)) {
 organisationRelationBuffer += OrganisationIsLocatedInPlaceData(
 src = organisationId,
 dst = placeIdmapping(cityId.toLong)
)
 }
 }

 logger.info(s"Processed ${organisationdataBuffer.size} Organisation entities")
 }

 /**
 * Generate relationships between static entities
 */
 private def generateStaticRelations(): Unit = {
 logger.info("Generating static entity relationships...")

 // Process Place hierarchical relationships (process deferred references)
 val pendingPlaceRelations = mutable.ListBuffer[PlaceIsPartOfPlaceData]()

 placedataBuffer.foreach { place =>
 place.partOfPlaceId.foreach { parentOriginalId =>
 if (placeIdmapping.contains(parentOriginalId)) {
 pendingPlaceRelations += PlaceIsPartOfPlaceData(
 src = place.id,
 dst = placeIdmapping(parentOriginalId)
)
 }
 }
 }

 placeRelationBuffer ++= pendingPlaceRelations

 logger.info(s"Generated static relationships: Place hierarchy ${placeRelationBuffer.size}, " +
 s"Tag classification ${tagRelationBuffer.size}, " +
 s"TagClass hierarchy ${tagClassRelationBuffer.size}, " +
 s"Organisation location ${organisationRelationBuffer.size}")
 }

 /**
 * batchwrite GraphAr format
 */
 private def writeStaticEntitiesToGraphAr(): Unit = {
 logger.info("Starting to write static entities to GraphAr format...")
 import sparkSession.implicits._

 try {
 // WriteVertexEntity
 if (placedataBuffer.nonEmpty) {
 writeVertexData("Place", placedataBuffer.toSeq)
 logger.info(s"Wrote Place vertices: ${placedataBuffer.size}")
 }

 if (tagdataBuffer.nonEmpty) {
 writeVertexData("Tag", tagdataBuffer.toSeq)
 logger.info(s"Wrote Tag vertices: ${tagdataBuffer.size}")
 }

 if (tagClassdataBuffer.nonEmpty) {
 writeVertexData("TagClass", tagClassdataBuffer.toSeq)
 logger.info(s"Wrote TagClass vertices: ${tagClassdataBuffer.size}")
 }

 if (organisationdataBuffer.nonEmpty) {
 writeVertexData("Organisation", organisationdataBuffer.toSeq)
 logger.info(s"Wrote Organisation vertices: ${organisationdataBuffer.size}")
 }

 // Writeedge relationship
 if (placeRelationBuffer.nonEmpty) {
 writeEdgeData("Place_isPartOf_Place", placeRelationBuffer.toSeq)
 logger.info(s"Wrote Place hierarchy relationships: ${placeRelationBuffer.size}")
 }

 if (tagRelationBuffer.nonEmpty) {
 writeEdgeData("Tag_hasType_TagClass", tagRelationBuffer.toSeq)
 logger.info(s"Wrote Tag classification relationships: ${tagRelationBuffer.size}")
 }

 if (tagClassRelationBuffer.nonEmpty) {
 writeEdgeData("TagClass_isSubclassOf_TagClass", tagClassRelationBuffer.toSeq)
 logger.info(s"Wrote TagClass hierarchy relationships: ${tagClassRelationBuffer.size}")
 }

 if (organisationRelationBuffer.nonEmpty) {
 writeEdgeData("Organisation_isLocatedIn_Place", organisationRelationBuffer.toSeq)
 logger.info(s"Wrote Organisation location relationships: ${organisationRelationBuffer.size}")
 }

 logger.info("✓ All static entity data written to GraphAr successfully")

 } catch {
 case e: Exception =>
 logger.error("Error occurred during GraphAr write process", e)
 throw e
 }
 }

 /**
 * WriteVertex datatoGraphArFormat
 */
 private def writeVertexData[T <: Product: TypeTag: ClassTag](entityType: String, data: Seq[T]): Unit = {
 import sparkSession.implicits._

 val rdd = sparkSession.sparkContext.parallelize(data)
 val df = rdd.toDF()
 val outputDir = s"$output_path/vertex/$entityType"

 // according tochunkSizeshard
 val totalRows = df.count()
 val numChunks = Math.ceil(totalRows.toDouble / vertex_chunk_size).toInt

 for (chunkIndex <- 0 until numChunks) {
 val startRow = chunkIndex * vertex_chunk_size
 val endRow = Math.min((chunkIndex + 1) * vertex_chunk_size, totalRows)

 // Use row_number for pagination
 import org.apache.spark.sql.expressions.Window
 val windowSpec = Window.orderBy(monotonically_increasing_id())
 val chunkDF = df.withColumn("row_number", row_number().over(windowSpec))
.filter(col("row_number") > startRow && col("row_number") <= endRow)
.drop("row_number")

 val chunkPath = s"$outputDir/chunk$chunkIndex"

 // according to file typeWrite
 file_type.toLowerCase match {
 case "parquet" => chunkDF.write.mode("overwrite").parquet(chunkPath)
 case "csv" => chunkDF.write.mode("overwrite").option("header", "true").csv(chunkPath)
 case "orc" => chunkDF.write.mode("overwrite").orc(chunkPath)
 case _ => chunkDF.write.mode("overwrite").parquet(chunkPath)
 }
 }
 }

 /**
 * WriteEdge datatoGraphArFormat
 */
 private def writeEdgeData[T <: Product: TypeTag: ClassTag](relationName: String, data: Seq[T]): Unit = {
 import sparkSession.implicits._

 val rdd = sparkSession.sparkContext.parallelize(data)
 val df = rdd.toDF()
 val outputDir = s"$output_path/edge/$relationName"

 // according tochunkSizeshard
 val totalRows = df.count()
 val numChunks = Math.ceil(totalRows.toDouble / edge_chunk_size).toInt

 for (chunkIndex <- 0 until numChunks) {
 val startRow = chunkIndex * edge_chunk_size
 val endRow = Math.min((chunkIndex + 1) * edge_chunk_size, totalRows)

 // Use row_number for pagination
 import org.apache.spark.sql.expressions.Window
 val windowSpec = Window.orderBy(monotonically_increasing_id())
 val chunkDF = df.withColumn("row_number", row_number().over(windowSpec))
.filter(col("row_number") > startRow && col("row_number") <= endRow)
.drop("row_number")

 val chunkPath = s"$outputDir/chunk$chunkIndex"

 // according to file typeWrite
 file_type.toLowerCase match {
 case "parquet" => chunkDF.write.mode("overwrite").parquet(chunkPath)
 case "csv" => chunkDF.write.mode("overwrite").option("header", "true").csv(chunkPath)
 case "orc" => chunkDF.write.mode("overwrite").orc(chunkPath)
 case _ => chunkDF.write.mode("overwrite").parquet(chunkPath)
 }
 }
 }

 /**
 * GetSupport Static entityType
 */
 def getSupportedStaticEntityTypes(): List[String] = {
 List("Place", "Tag", "TagClass", "Organisation")
 }

 /**
 * GetSupport StaticrelationshipType
 */
 def getSupportedStaticRelationTypes(): List[String] = {
 List(
 "Place_isPartOf_Place",
 "Tag_hasType_TagClass",
 "TagClass_isSubclassOf_TagClass",
 "Organisation_isLocatedIn_Place"
)
 }

 /**
 * GetConvertStatistics
 */
 def getConversionStatistics(): Map[String, Int] = {
 Map(
 "placeCount" -> placedataBuffer.size,
 "tagCount" -> tagdataBuffer.size,
 "tagClassCount" -> tagClassdataBuffer.size,
 "organisationCount" -> organisationdataBuffer.size,
 "placeRelationCount" -> placeRelationBuffer.size,
 "tagRelationCount" -> tagRelationBuffer.size,
 "tagClassRelationCount" -> tagClassRelationBuffer.size,
 "organisationRelationCount" -> organisationRelationBuffer.size
)
 }

 /**
 * Closeoutput stream,Clean up resources
 */
 override def close(): Unit = {
 try {
 // Clean upCache
 placedataBuffer.clear()
 tagdataBuffer.clear()
 tagClassdataBuffer.clear()
 organisationdataBuffer.clear()

 placeRelationBuffer.clear()
 tagRelationBuffer.clear()
 tagClassRelationBuffer.clear()
 organisationRelationBuffer.clear()

 // Clean up mapping tables
 placeIdmapping.clear()
 tagIdmapping.clear()
 tagClassIdmapping.clear()
 organisationIdmapping.clear()
 exportedTagClasses.clear()

 logger.info("GraphArStaticOutputStream resource cleanup completed")

 } catch {
 case e: Exception =>
 logger.error("Error occurred while closing GraphArStaticOutputStream", e)
 }
 }

 /**
 * LDBC standard resource management pattern - use try-with-resources
 */
 def use[T](f: GraphArStaticOutputStream => T): T = {
 try {
 f(this)
 } finally {
 close()
 }
 }
}