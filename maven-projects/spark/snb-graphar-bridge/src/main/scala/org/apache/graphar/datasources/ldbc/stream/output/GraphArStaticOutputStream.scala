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
 * GraphAr静态实体输出流
 *
 * 拦截LDBC的RecordOutputStream[StaticGraph.type]，将静态实体数据转换为GraphAr格式。
 * 这个类实现了与动态实体处理的架构对称性，复用流式拦截机制处理静态数据。
 *
 * 支持的静态实体：
 * - Place：地理位置实体（国家、城市等）
 * - Tag：标签实体
 * - TagClass：标签分类实体
 * - Organisation：组织机构实体（公司、大学）
 *
 * 支持的静态关系：
 * - Place-isPartOf-Place：地理层次关系
 * - Tag-hasType-TagClass：标签分类关系
 * - TagClass-isSubclassOf-TagClass：分类层次关系
 * - Organisation-isLocatedIn-Place：组织位置关系
 */
class GraphArStaticOutputStream(
  outputPath: String,
  graphName: String,
  vertexChunkSize: Long = 1024L,
  edgeChunkSize: Long = 1024L,
  fileType: String = "parquet"
)(@transient implicit val spark: SparkSession) extends RecordOutputStream[StaticGraph.type] with Serializable {

  @transient private val logger: Logger = LoggerFactory.getLogger(classOf[GraphArStaticOutputStream])

  // 懒初始化SparkSession以处理反序列化
  @transient private lazy val sparkSession: SparkSession = {
    if (spark != null) spark else SparkSession.active
  }

  // 连续ID生成器，确保GraphAr格式要求的连续ID
  private val placeIdGenerator = new ContinuousIdGenerator()
  private val tagIdGenerator = new ContinuousIdGenerator()
  private val tagClassIdGenerator = new ContinuousIdGenerator()
  private val organisationIdGenerator = new ContinuousIdGenerator()

  // 原始ID到连续ID的映射表，用于处理实体间关系
  private val placeIdMapping = mutable.Map[Long, Long]()
  private val tagIdMapping = mutable.Map[Int, Long]() // Tag ID是Integer类型
  private val tagClassIdMapping = mutable.Map[Int, Long]()
  private val organisationIdMapping = mutable.Map[Int, Long]()

  // 已导出的TagClass集合，避免重复导出
  private val exportedTagClasses = mutable.Set[Int]()

  // 数据缓存，用于批量写入GraphAr
  private val placeDataBuffer = mutable.ListBuffer[PlaceData]()
  private val tagDataBuffer = mutable.ListBuffer[TagData]()
  private val tagClassDataBuffer = mutable.ListBuffer[TagClassData]()
  private val organisationDataBuffer = mutable.ListBuffer[OrganisationData]()

  // 关系数据缓存
  private val placeRelationBuffer = mutable.ListBuffer[PlaceIsPartOfPlaceData]()
  private val tagRelationBuffer = mutable.ListBuffer[TagHasTypeTagClassData]()
  private val tagClassRelationBuffer = mutable.ListBuffer[TagClassIsSubclassOfTagClassData]()
  private val organisationRelationBuffer = mutable.ListBuffer[OrganisationIsLocatedInPlaceData]()

  logger.info(s"GraphArStaticOutputStream初始化完成: outputPath=$outputPath, fileType=$fileType")

  /**
   * 拦截静态实体数据写入的核心方法
   *
   * 当LDBC生成器调用StaticOutputStream.write(StaticGraph)时，
   * 这个方法会被触发，我们在此处拦截并转换数据
   */
  override def write(staticGraph: StaticGraph.type): Unit = {
    logger.info("=== 开始拦截静态实体数据并转换为GraphAr格式 ===")

    try {
      // 按照StaticOutputStream.scala的逻辑处理各类静态实体
      processPlaceEntities()           // Place + Place层次关系
      processTagEntities()             // Tag + TagClass层次关系
      processOrganisationEntities()    // Company + University
      generateStaticRelations()       // 静态实体间关系

      // 批量写入GraphAr格式
      writeStaticEntitiesToGraphAr()

      logger.info("✓ 静态实体GraphAr转换完成")

    } catch {
      case e: Exception =>
        logger.error("静态实体转换过程中发生错误", e)
        throw e
    }
  }

  /**
   * 处理Place实体
   * 复用StaticOutputStream.scala:61-67的逻辑
   */
  private def processPlaceEntities(): Unit = {
    logger.info("开始处理Place实体...")

    // 验证Dictionaries.places是否已初始化
    if (Dictionaries.places == null) {
      logger.error("Dictionaries.places未初始化，无法处理Place实体")
      return
    }

    val places = Dictionaries.places.getPlaces
    if (places == null || places.isEmpty) {
      logger.warn("Places字典为空，跳过Place实体处理")
      return
    }

    logger.info(s"Places字典包含 ${places.size()} 个地点")
    val locations = places.iterator().asScala

    for { location <- locations } {
      val place = Dictionaries.places.getLocation(location)
      if (place != null) {
        place.setName(StringUtils.clampString(place.getName, 256))

        val placeId = placeIdGenerator.getNextId("Place", place.getId)
        placeIdMapping(place.getId) = placeId

        val partOfPlaceId = place.getType match {
          case Place.CITY | Place.COUNTRY =>
            val parentPlaceId = Dictionaries.places.belongsTo(place.getId)
            if (parentPlaceId != -1) Some(parentPlaceId) else None
          case _ => None
        }

        val placeData = PlaceData(
          id = placeId,
          originalId = place.getId,
          name = place.getName,
          url = DBP.getUrl(place.getName),
          placeType = place.getType, // Place.getType返回String类型
          partOfPlaceId = partOfPlaceId.map(_.toLong) // 转换为Long类型
        )

        placeDataBuffer += placeData

        // 处理Place层次关系
        partOfPlaceId.foreach { parentId =>
          // 确保父级Place已经处理
          if (placeIdMapping.contains(parentId)) {
            placeRelationBuffer += PlaceIsPartOfPlaceData(
              src = placeId,
              dst = placeIdMapping(parentId)
            )
          }
        }
      }
    }

    logger.info(s"✓ 成功处理了 ${placeDataBuffer.size} 个Place实体")
  }

  /**
   * 处理Tag实体和TagClass层次结构
   * 复用StaticOutputStream.scala:69-83的逻辑
   */
  private def processTagEntities(): Unit = {
    logger.info("开始处理Tag实体...")

    val tags = Dictionaries.tags.getTags.iterator().asScala

    for { tag <- tags } {
      val tagName = StringUtils
        .clampString(Dictionaries.tags.getName(tag), 256)
        .replace("\"", "\\\"")

      val tagId = tagIdGenerator.getNextId("Tag", tag.toLong)
      tagIdMapping(tag) = tagId

      val tagData = TagData(
        id = tagId,
        originalId = tag.toLong, // 转换为Long类型
        name = tagName,
        url = DBP.getUrl(tagName),
        typeTagClassId = Dictionaries.tags.getTagClass(tag)
      )

      tagDataBuffer += tagData

      // 处理TagClass层次结构
      processTagClassHierarchy(tagData)

      // 处理Tag-TagClass关系
      val tagClassId = Dictionaries.tags.getTagClass(tag)
      if (tagClassIdMapping.contains(tagClassId)) {
        tagRelationBuffer += TagHasTypeTagClassData(
          src = tagId,
          dst = tagClassIdMapping(tagClassId)
        )
      }
    }

    logger.info(s"处理了 ${tagDataBuffer.size} 个Tag实体")
  }

  /**
   * 处理TagClass层次结构
   * 复用StaticOutputStream.scala:43-58的逻辑
   */
  private def processTagClassHierarchy(tag: TagData): Unit = {
    var classId = tag.typeTagClassId

    while (classId != -1 && !exportedTagClasses.contains(classId)) {
      exportedTagClasses.add(classId)

      val classParent = Dictionaries.tags.getClassParent(classId)
      val className = StringUtils.clampString(Dictionaries.tags.getClassName(classId), 256)

      val tagClassId = tagClassIdGenerator.getNextId("TagClass", classId)
      tagClassIdMapping(classId) = tagClassId

      val tagClassData = TagClassData(
        id = tagClassId,
        originalId = classId,
        name = className,
        url = if (className == "Thing") "http://www.w3.org/2002/07/owl#Thing" else DBPOWL.getUrl(className),
        subclassOfId = if (classParent != -1) Some(classParent) else None
      )

      tagClassDataBuffer += tagClassData

      // 处理TagClass层次关系
      if (classParent != -1 && tagClassIdMapping.contains(classParent)) {
        tagClassRelationBuffer += TagClassIsSubclassOfTagClassData(
          src = tagClassId,
          dst = tagClassIdMapping(classParent)
        )
      }

      classId = classParent
    }
  }

  /**
   * 处理Organisation实体（Company + University）
   * 复用StaticOutputStream.scala:85-112的逻辑
   */
  private def processOrganisationEntities(): Unit = {
    logger.info("开始处理Organisation实体...")

    // 处理公司数据
    val companies = Dictionaries.companies.getCompanies.iterator().asScala
    for { company <- companies } {
      val companyName = StringUtils.clampString(Dictionaries.companies.getCompanyName(company), 256)
      val organisationId = organisationIdGenerator.getNextId("Organisation", company.toInt)
      organisationIdMapping(company.toInt) = organisationId

      val organisationData = OrganisationData(
        id = organisationId,
        originalId = company.toInt,
        organisationType = Organisation.OrganisationType.Company.toString,
        name = companyName,
        url = DBP.getUrl(companyName),
        locationId = Dictionaries.companies.getCountry(company)
      )

      organisationDataBuffer += organisationData

      // 处理Organisation-Place关系
      val countryId = Dictionaries.companies.getCountry(company)
      if (placeIdMapping.contains(countryId.toLong)) {
        organisationRelationBuffer += OrganisationIsLocatedInPlaceData(
          src = organisationId,
          dst = placeIdMapping(countryId.toLong)
        )
      }
    }

    // 处理大学数据
    val universities = Dictionaries.universities.getUniversities.iterator().asScala
    for { university <- universities } {
      val universityName = StringUtils.clampString(Dictionaries.universities.getUniversityName(university), 256)
      val organisationId = organisationIdGenerator.getNextId("Organisation", university.toInt)
      organisationIdMapping(university.toInt) = organisationId

      val organisationData = OrganisationData(
        id = organisationId,
        originalId = university.toInt,
        organisationType = Organisation.OrganisationType.University.toString,
        name = universityName,
        url = DBP.getUrl(universityName),
        locationId = Dictionaries.universities.getUniversityCity(university)
      )

      organisationDataBuffer += organisationData

      // 处理Organisation-Place关系
      val cityId = Dictionaries.universities.getUniversityCity(university)
      if (placeIdMapping.contains(cityId.toLong)) {
        organisationRelationBuffer += OrganisationIsLocatedInPlaceData(
          src = organisationId,
          dst = placeIdMapping(cityId.toLong)
        )
      }
    }

    logger.info(s"处理了 ${organisationDataBuffer.size} 个Organisation实体")
  }

  /**
   * 生成静态实体间关系
   */
  private def generateStaticRelations(): Unit = {
    logger.info("生成静态实体间关系...")

    // 后处理Place层次关系（处理延迟引用）
    val pendingPlaceRelations = mutable.ListBuffer[PlaceIsPartOfPlaceData]()

    placeDataBuffer.foreach { place =>
      place.partOfPlaceId.foreach { parentOriginalId =>
        if (placeIdMapping.contains(parentOriginalId)) {
          pendingPlaceRelations += PlaceIsPartOfPlaceData(
            src = place.id,
            dst = placeIdMapping(parentOriginalId)
          )
        }
      }
    }

    placeRelationBuffer ++= pendingPlaceRelations

    logger.info(s"生成了静态关系: Place层次${placeRelationBuffer.size}个, " +
               s"Tag分类${tagRelationBuffer.size}个, " +
               s"TagClass层次${tagClassRelationBuffer.size}个, " +
               s"Organisation位置${organisationRelationBuffer.size}个")
  }

  /**
   * 批量写入GraphAr格式
   */
  private def writeStaticEntitiesToGraphAr(): Unit = {
    logger.info("开始写入静态实体到GraphAr格式...")
    import sparkSession.implicits._

    try {
      // 写入顶点实体
      if (placeDataBuffer.nonEmpty) {
        writeVertexData("Place", placeDataBuffer.toSeq)
        logger.info(s"写入Place顶点: ${placeDataBuffer.size}个")
      }

      if (tagDataBuffer.nonEmpty) {
        writeVertexData("Tag", tagDataBuffer.toSeq)
        logger.info(s"写入Tag顶点: ${tagDataBuffer.size}个")
      }

      if (tagClassDataBuffer.nonEmpty) {
        writeVertexData("TagClass", tagClassDataBuffer.toSeq)
        logger.info(s"写入TagClass顶点: ${tagClassDataBuffer.size}个")
      }

      if (organisationDataBuffer.nonEmpty) {
        writeVertexData("Organisation", organisationDataBuffer.toSeq)
        logger.info(s"写入Organisation顶点: ${organisationDataBuffer.size}个")
      }

      // 写入边关系
      if (placeRelationBuffer.nonEmpty) {
        writeEdgeData("Place_isPartOf_Place", placeRelationBuffer.toSeq)
        logger.info(s"写入Place层次关系: ${placeRelationBuffer.size}个")
      }

      if (tagRelationBuffer.nonEmpty) {
        writeEdgeData("Tag_hasType_TagClass", tagRelationBuffer.toSeq)
        logger.info(s"写入Tag分类关系: ${tagRelationBuffer.size}个")
      }

      if (tagClassRelationBuffer.nonEmpty) {
        writeEdgeData("TagClass_isSubclassOf_TagClass", tagClassRelationBuffer.toSeq)
        logger.info(s"写入TagClass层次关系: ${tagClassRelationBuffer.size}个")
      }

      if (organisationRelationBuffer.nonEmpty) {
        writeEdgeData("Organisation_isLocatedIn_Place", organisationRelationBuffer.toSeq)
        logger.info(s"写入Organisation位置关系: ${organisationRelationBuffer.size}个")
      }

      logger.info("✓ 所有静态实体数据写入GraphAr完成")

    } catch {
      case e: Exception =>
        logger.error("写入GraphAr过程中发生错误", e)
        throw e
    }
  }

  /**
   * 写入顶点数据到GraphAr格式
   */
  private def writeVertexData[T <: Product : TypeTag : ClassTag](entityType: String, data: Seq[T]): Unit = {
    import sparkSession.implicits._

    val rdd = sparkSession.sparkContext.parallelize(data)
    val df = rdd.toDF()
    val outputDir = s"$outputPath/vertex/$entityType"

    // 根据chunk大小分片
    val totalRows = df.count()
    val numChunks = Math.ceil(totalRows.toDouble / vertexChunkSize).toInt

    for (chunkIndex <- 0 until numChunks) {
      val startRow = chunkIndex * vertexChunkSize
      val endRow = Math.min((chunkIndex + 1) * vertexChunkSize, totalRows)

      // 使用row_number进行分页
      import org.apache.spark.sql.expressions.Window
      val windowSpec = Window.orderBy(monotonically_increasing_id())
      val chunkDF = df.withColumn("row_number", row_number().over(windowSpec))
        .filter(col("row_number") > startRow && col("row_number") <= endRow)
        .drop("row_number")

      val chunkPath = s"$outputDir/chunk$chunkIndex"

      // 根据文件类型写入
      fileType.toLowerCase match {
        case "parquet" => chunkDF.write.mode("overwrite").parquet(chunkPath)
        case "csv" => chunkDF.write.mode("overwrite").option("header", "true").csv(chunkPath)
        case "orc" => chunkDF.write.mode("overwrite").orc(chunkPath)
        case _ => chunkDF.write.mode("overwrite").parquet(chunkPath)
      }
    }
  }

  /**
   * 写入边数据到GraphAr格式
   */
  private def writeEdgeData[T <: Product : TypeTag : ClassTag](relationName: String, data: Seq[T]): Unit = {
    import sparkSession.implicits._

    val rdd = sparkSession.sparkContext.parallelize(data)
    val df = rdd.toDF()
    val outputDir = s"$outputPath/edge/$relationName"

    // 根据chunk大小分片
    val totalRows = df.count()
    val numChunks = Math.ceil(totalRows.toDouble / edgeChunkSize).toInt

    for (chunkIndex <- 0 until numChunks) {
      val startRow = chunkIndex * edgeChunkSize
      val endRow = Math.min((chunkIndex + 1) * edgeChunkSize, totalRows)

      // 使用row_number进行分页
      import org.apache.spark.sql.expressions.Window
      val windowSpec = Window.orderBy(monotonically_increasing_id())
      val chunkDF = df.withColumn("row_number", row_number().over(windowSpec))
        .filter(col("row_number") > startRow && col("row_number") <= endRow)
        .drop("row_number")

      val chunkPath = s"$outputDir/chunk$chunkIndex"

      // 根据文件类型写入
      fileType.toLowerCase match {
        case "parquet" => chunkDF.write.mode("overwrite").parquet(chunkPath)
        case "csv" => chunkDF.write.mode("overwrite").option("header", "true").csv(chunkPath)
        case "orc" => chunkDF.write.mode("overwrite").orc(chunkPath)
        case _ => chunkDF.write.mode("overwrite").parquet(chunkPath)
      }
    }
  }

  /**
   * 获取支持的静态实体类型
   */
  def getSupportedStaticEntityTypes(): List[String] = {
    List("Place", "Tag", "TagClass", "Organisation")
  }

  /**
   * 获取支持的静态关系类型
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
   * 获取转换统计信息
   */
  def getConversionStatistics(): Map[String, Int] = {
    Map(
      "placeCount" -> placeDataBuffer.size,
      "tagCount" -> tagDataBuffer.size,
      "tagClassCount" -> tagClassDataBuffer.size,
      "organisationCount" -> organisationDataBuffer.size,
      "placeRelationCount" -> placeRelationBuffer.size,
      "tagRelationCount" -> tagRelationBuffer.size,
      "tagClassRelationCount" -> tagClassRelationBuffer.size,
      "organisationRelationCount" -> organisationRelationBuffer.size
    )
  }

  /**
   * 关闭输出流，清理资源
   */
  override def close(): Unit = {
    try {
      // 清理缓存
      placeDataBuffer.clear()
      tagDataBuffer.clear()
      tagClassDataBuffer.clear()
      organisationDataBuffer.clear()

      placeRelationBuffer.clear()
      tagRelationBuffer.clear()
      tagClassRelationBuffer.clear()
      organisationRelationBuffer.clear()

      // 清理映射表
      placeIdMapping.clear()
      tagIdMapping.clear()
      tagClassIdMapping.clear()
      organisationIdMapping.clear()
      exportedTagClasses.clear()

      logger.info("GraphArStaticOutputStream资源清理完成")

    } catch {
      case e: Exception =>
        logger.error("关闭GraphArStaticOutputStream时发生错误", e)
    }
  }

  /**
   * LDBC标准的资源管理模式 - use方法
   */
  def use[T](f: GraphArStaticOutputStream => T): T = {
    try {
      f(this)
    } finally {
      close()
    }
  }
}