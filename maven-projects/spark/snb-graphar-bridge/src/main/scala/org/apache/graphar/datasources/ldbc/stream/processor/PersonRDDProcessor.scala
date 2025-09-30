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

import org.apache.graphar.datasources.ldbc.stream.core.GraphArDataCollector
import org.apache.graphar.datasources.ldbc.model.{ValidationResult, ValidationSuccess, ValidationFailure}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.Try
import scala.collection.JavaConverters._
import ldbc.snb.datagen.generator.dictionary.Dictionaries

/** PersonRDDProcessor伴生对象 - 提供静态工具方法 */
object PersonRDDProcessor {
  // 语言列表转字符串（避免序列化问题）
  def getLanguages(languages: java.util.List[Integer]): String = {
    if (languages != null) languages.asScala.map(_.toString).mkString(";") else ""
  }

  // 邮箱列表转字符串（避免序列化问题）
  def getEmails(emails: java.util.List[String]): String = {
    if (emails != null) emails.asScala.mkString(";") else ""
  }
}

/** 基于RDD的Person处理器，处理静态Person实体及关系 */
class PersonRDDProcessor(
                          idManager: UnifiedIdManager,
                          config: Map[String, String] = Map.empty
                        )(implicit spark: SparkSession) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[PersonRDDProcessor])

  // Person DataFrame标准Schema
  private val personSchema = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("firstName", StringType, nullable = true),
    StructField("lastName", StringType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("birthday", StringType, nullable = true), // 用String避免DateType问题
    StructField("creationDate", TimestampType, nullable = false),
    StructField("locationIP", StringType, nullable = true),
    StructField("browserUsed", StringType, nullable = true),
    StructField("cityId", LongType, nullable = true),
    StructField("languages", StringType, nullable = true),
    StructField("emails", StringType, nullable = true)
  ))

  // 处理统计信息
  private val processingStats = mutable.Map[String, Long]()

  // 缓存Person RDD，避免重复生成
  private lazy val cachedPersonRDD = {
    logger.info("生成并缓存Person RDD（含knows关系）")
    val ldbcConfig = createLdbcConfiguration()

    import ldbc.snb.datagen.generator.generators._
    import ldbc.snb.datagen.generator.DatagenParams
    import ldbc.snb.datagen.entities.Keys._

    DatagenParams.readConf(ldbcConfig)
    Dictionaries.loadDictionaries()

    // 步骤1: 生成Person对象
    logger.info("步骤1: 生成Person对象")
    val personRDD = SparkPersonGenerator(ldbcConfig)(spark)

    // 步骤2: 生成3种knows关系（45%大学 + 45%兴趣 + 10%随机）
    logger.info("步骤2: 生成knows关系（45%大学 + 45%兴趣 + 10%随机）")
    val percentages = Seq(0.45f, 0.45f, 0.1f)
    val knowsGeneratorClassName = DatagenParams.getKnowsGenerator

    val uniRanker = SparkRanker.create(_.byUni)
    val interestRanker = SparkRanker.create(_.byInterest)
    val randomRanker = SparkRanker.create(_.byRandomId)

    logger.info("  - 生成基于大学的knows关系")
    val uniKnows = SparkKnowsGenerator(personRDD, uniRanker, ldbcConfig, percentages, 0, knowsGeneratorClassName)
    logger.info("  - 生成基于兴趣的knows关系")
    val interestKnows = SparkKnowsGenerator(personRDD, interestRanker, ldbcConfig, percentages, 1, knowsGeneratorClassName)
    logger.info("  - 生成随机knows关系")
    val randomKnows = SparkKnowsGenerator(personRDD, randomRanker, ldbcConfig, percentages, 2, knowsGeneratorClassName)

    // 步骤3: 合并所有knows关系
    logger.info("步骤3: 合并所有knows关系")
    val personRDDWithKnows = SparkKnowsMerger(uniKnows, interestKnows, randomKnows)

    import org.apache.spark.storage.StorageLevel
    val cached = personRDDWithKnows.persist(StorageLevel.MEMORY_AND_DISK_SER)

    logger.info(s"Person RDD（含knows关系）缓存完成，存储级别: MEMORY_AND_DISK_SER")
    cached
  }

  logger.info("PersonRDDProcessor初始化")

  /** 处理Person实体数据 */
  def processPersonFromRDD(): Try[DataFrame] = {
    Try {
      logger.info("处理Person RDD数据")
      val personDF = generatePersonDataFrame()
      val personWithIds = idManager.assignVertexIds(personDF, "Person")

      val recordCount = personWithIds.count()
      processingStats("person_count") = recordCount
      logger.info(s"Person处理完成: 记录数=$recordCount")

      personWithIds
    }.recover {
      case e: Exception =>
        logger.error("Person处理失败", e)
        throw e
    }
  }

  /** 处理Person相关关系数据 */
  def processPersonRelationsFromRDD(): Try[Map[String, DataFrame]] = {
    Try {
      logger.info("处理Person关系数据")
      val relations = mutable.Map[String, DataFrame]()

      // 处理knows关系
      val knowsDF = processKnowsRelation()
      relations("knows") = knowsDF
      processingStats("knows_count") = knowsDF.count()

      // 处理hasInterest关系
      val hasInterestDF = processHasInterestRelation()
      relations("hasInterest") = hasInterestDF
      processingStats("hasInterest_count") = hasInterestDF.count()

      // 处理studyAt关系
      val studyAtDF = processStudyAtRelation()
      relations("studyAt") = studyAtDF
      processingStats("studyAt_count") = studyAtDF.count()

      // 处理workAt关系
      val workAtDF = processWorkAtRelation()
      relations("workAt") = workAtDF
      processingStats("workAt_count") = workAtDF.count()

      logger.info(s"Person关系处理完成: knows=${processingStats("knows_count")}, " +
        s"hasInterest=${processingStats("hasInterest_count")}, " +
        s"studyAt=${processingStats("studyAt_count")}, " +
        s"workAt=${processingStats("workAt_count")}")

      relations.toMap
    }.recover {
      case e: Exception =>
        logger.error("Person关系处理失败", e)
        throw e
    }
  }

  /** 处理Person数据并添加到数据收集器 */
  def processAndCollect(dataCollector: GraphArDataCollector): Try[PersonProcessingResult] = {
    Try {
      logger.info("处理Person数据并收集到GraphArDataCollector")

      // 处理Person实体
      val personDF = processPersonFromRDD().get
      logger.info(s"Person添加到DataCollector，记录数: ${personDF.count()}")
      dataCollector.addStaticVertexData("Person", personDF)

      // 处理Person关系
      val relations = processPersonRelationsFromRDD().get
      logger.info(s"处理Person关系，类型数: ${relations.size}")

      relations.foreach { case (relationType, df) =>
        val count = df.count()
        logger.info(s"处理关系 $relationType，记录数: $count")
        relationType match {
          case "knows" => dataCollector.addStaticEdgeData(("Person", "knows", "Person"), df)
          case "hasInterest" => dataCollector.addStaticEdgeData(("Person", "hasInterest", "Tag"), df)
          case "studyAt" => dataCollector.addStaticEdgeData(("Person", "studyAt", "Organisation"), df)
          case "workAt" => dataCollector.addStaticEdgeData(("Person", "workAt", "Organisation"), df)
          case _ => logger.warn(s"未知关系类型: $relationType")
        }
      }

      val result = PersonProcessingResult(
        personCount = processingStats("person_count"),
        knowsCount = processingStats("knows_count"),
        hasInterestCount = processingStats("hasInterest_count"),
        studyAtCount = processingStats("studyAt_count"),
        workAtCount = processingStats("workAt_count"),
        success = true
      )

      logger.info(s"Person处理和收集完成: $result")
      result
    }.recover {
      case e: Exception =>
        logger.error("Person处理和收集失败", e)
        PersonProcessingResult(0, 0, 0, 0, 0, success = false, error = Some(e.getMessage))
    }
  }

  /** 从缓存RDD生成Person DataFrame */
  private def generatePersonDataFrame(): DataFrame = {
    logger.info("从缓存RDD生成Person DataFrame")
    import scala.collection.JavaConverters._
    val personDF = spark.createDataFrame(cachedPersonRDD.map { person =>
      org.apache.spark.sql.Row(
        person.getAccountId().toLong,
        person.getFirstName(),
        person.getLastName(),
        if (person.getGender() == 1) "male" else "female",
        new java.sql.Date(person.getBirthday()).toString,
        new java.sql.Timestamp(person.getCreationDate()),
        person.getIpAddress().toString,
        Dictionaries.browsers.getName(person.getBrowserId()),
        person.getCityId().toLong,
        PersonRDDProcessor.getLanguages(person.getLanguages()),
        PersonRDDProcessor.getEmails(person.getEmails())
      )
    }, personSchema)

    logger.info("Person DataFrame生成成功")
    personDF
  }

  /** 创建LDBC配置 */
  private def createLdbcConfiguration(): ldbc.snb.datagen.util.GeneratorConfiguration = {
    import ldbc.snb.datagen.util.{GeneratorConfiguration, ConfigParser}
    import scala.collection.JavaConverters._

    val scaleFactor = spark.conf.getOption("ldbc.scale.factor").map(_.toDouble).getOrElse(0.003)
    val config = org.apache.graphar.datasources.ldbc.util.LdbcConfigUtils.createFastTestConfig(scaleFactor.toString)

    logger.info(s"LDBC配置创建: scaleFactor=$scaleFactor")
    config
  }

  /** 处理knows关系（使用LDBC的Person.getKnows()） */
  private def processKnowsRelation(): DataFrame = {
    logger.info("生成knows关系数据")
    val knowsSchema = StructType(Array(
      StructField("src", LongType, nullable = false),
      StructField("dst", LongType, nullable = false),
      StructField("creationDate", TimestampType, nullable = false),
      StructField("weight", DoubleType, nullable = true)
    ))

    import scala.collection.JavaConverters._
    val knowsRows = cachedPersonRDD.flatMap { person =>
      val personId = person.getAccountId().toLong
      person.getKnows().asScala.map { knows =>
        org.apache.spark.sql.Row(
          personId,
          knows.getTo().getAccountId().toLong,
          new java.sql.Timestamp(knows.getCreationDate()),
          knows.getWeight().toDouble
        )
      }
    }

    val knowsDF = spark.createDataFrame(knowsRows, knowsSchema).coalesce(1)
    logger.info(s"生成knows关系 ${knowsDF.count()} 条")
    knowsDF
  }

  /** 处理hasInterest关系（使用LDBC的Person.getInterests()） */
  private def processHasInterestRelation(): DataFrame = {
    logger.info("生成hasInterest关系数据")
    val hasInterestSchema = StructType(Array(
      StructField("src", LongType, nullable = false),
      StructField("dst", LongType, nullable = false),
      StructField("creationDate", TimestampType, nullable = false)
    ))

    import scala.collection.JavaConverters._
    val hasInterestRows = cachedPersonRDD.flatMap { person =>
      val personId = person.getAccountId().toLong
      person.getInterests().asScala.map { tagId =>
        org.apache.spark.sql.Row(
          personId,
          tagId.toLong,
          new java.sql.Timestamp(person.getCreationDate())
        )
      }
    }

    val hasInterestDF = spark.createDataFrame(hasInterestRows, hasInterestSchema).coalesce(1)
    logger.info(s"生成hasInterest关系 ${hasInterestDF.count()} 条")
    hasInterestDF
  }

  /** 处理studyAt关系（使用LDBC官方解码逻辑） */
  private def processStudyAtRelation(): DataFrame = {
    logger.info("生成studyAt关系数据")
    val studyAtSchema = StructType(Array(
      StructField("src", LongType, nullable = false),
      StructField("dst", LongType, nullable = false),
      StructField("classYear", IntegerType, nullable = false)
    ))

    val studyAtRows = cachedPersonRDD.flatMap { person =>
      val personId = person.getAccountId().toLong
      val universityLocationId = person.getUniversityLocationId()
      val classYear = person.getClassYear()
      val universityId = Dictionaries.universities.getUniversityFromLocation(universityLocationId)

      if (universityId != -1 && classYear != -1) {
        Some(org.apache.spark.sql.Row(personId, universityId, classYear.toInt))
      } else None
    }

    val studyAtDF = spark.createDataFrame(studyAtRows, studyAtSchema).coalesce(1)
    logger.info(s"生成studyAt关系 ${studyAtDF.count()} 条")
    studyAtDF
  }

  /** 处理workAt关系（使用LDBC的Person.getCompanies()） */
  private def processWorkAtRelation(): DataFrame = {
    logger.info("生成workAt关系数据")
    val workAtSchema = StructType(Array(
      StructField("src", LongType, nullable = false),
      StructField("dst", LongType, nullable = false),
      StructField("workFrom", StringType, nullable = false)
    ))

    import scala.collection.JavaConverters._
    val workAtRows = cachedPersonRDD.flatMap { person =>
      val personId = person.getAccountId().toLong
      person.getCompanies().asScala.map { case (companyId, workFromYear) =>
        org.apache.spark.sql.Row(
          personId,
          companyId,
          s"$workFromYear-01-01"
        )
      }
    }

    val workAtDF = spark.createDataFrame(workAtRows, workAtSchema).coalesce(1)
    logger.info(s"生成workAt关系 ${workAtDF.count()} 条")
    workAtDF
  }

  /** 获取处理统计信息 */
  def getProcessingStatistics(): Map[String, Long] = processingStats.toMap

  /** 验证Person数据完整性 */
  def validatePersonData(personDF: DataFrame): org.apache.graphar.datasources.ldbc.model.ValidationResult = {
    try {
      val validationErrors = mutable.ListBuffer[String]()
      val requiredFields = Set("id", "firstName", "lastName", "creationDate")
      val actualFields = personDF.columns.toSet

      // 检查必需字段
      requiredFields.foreach { field =>
        if (!actualFields.contains(field)) validationErrors += s"缺少字段: $field"
      }

      // 检查ID唯一性
      val totalCount = personDF.count()
      val uniqueIdCount = personDF.select("id").distinct().count()
      if (totalCount != uniqueIdCount) {
        validationErrors += s"ID不唯一: 总数=$totalCount, 唯一数=$uniqueIdCount"
      }

      // 检查NULL值
      requiredFields.foreach { field =>
        if (actualFields.contains(field)) {
          val nullCount = personDF.filter(col(field).isNull).count()
          if (nullCount > 0) validationErrors += s"字段 $field 有 $nullCount 个NULL值"
        }
      }

      if (validationErrors.isEmpty) ValidationSuccess
      else ValidationFailure(validationErrors.toList)
    } catch {
      case e: Exception =>
        logger.error("数据验证失败", e)
        ValidationFailure(List(s"验证出错: ${e.getMessage}"))
    }
  }
}

/** Person处理结果封装 */
case class PersonProcessingResult(
                                   personCount: Long,
                                   knowsCount: Long,
                                   hasInterestCount: Long,
                                   studyAtCount: Long,
                                   workAtCount: Long,
                                   success: Boolean,
                                   error: Option[String] = None
                                 )