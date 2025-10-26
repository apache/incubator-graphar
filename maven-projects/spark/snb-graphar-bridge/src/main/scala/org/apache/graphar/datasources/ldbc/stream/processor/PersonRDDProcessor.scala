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
import org.apache.graphar.datasources.ldbc.model.{
  ValidationResult,
  ValidationSuccess,
  ValidationFailure
}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.util.Try
import scala.collection.JavaConverters._
import ldbc.snb.datagen.generator.dictionary.Dictionaries

/** PersonRDDProcessor companion object - provides static utility methods */
object PersonRDDProcessor {

  /**
   * Convert language list to string (avoid serialization issues)
   *
   * @param languages
   *   Java list of language IDs
   * @return
   *   Semicolon-separated string of language IDs
   */
  def getLanguages(languages: java.util.List[Integer]): String = {
    if (languages != null) languages.asScala.map(_.toString).mkString(";")
    else ""
  }

  /**
   * Convert email list to string (avoid serialization issues)
   *
   * @param emails
   *   Java list of email addresses
   * @return
   *   Semicolon-separated string of emails
   */
  def getEmails(emails: java.util.List[String]): String = {
    if (emails != null) emails.asScala.mkString(";") else ""
  }
}

/**
 * RDD-based Person processor, processing static Person entities and
 * relationships
 */
class PersonRDDProcessor(
    idManager: UnifiedIdManager,
    config: Map[String, String] = Map.empty
)(implicit spark: SparkSession)
    extends Serializable {

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[PersonRDDProcessor])

  // Person DataFrame standard Schema
  private val personSchema = StructType(
    Array(
      StructField("id", LongType, nullable = false),
      StructField("firstName", StringType, nullable = true),
      StructField("lastName", StringType, nullable = true),
      StructField("gender", StringType, nullable = true),
      StructField(
        "birthday",
        StringType,
        nullable = true
      ), // Use String to avoid DateType issues
      StructField("creationDate", TimestampType, nullable = false),
      StructField("locationIP", StringType, nullable = true),
      StructField("browserUsed", StringType, nullable = true),
      StructField("cityId", LongType, nullable = true),
      StructField("languages", StringType, nullable = true),
      StructField("emails", StringType, nullable = true)
    )
  )

  // Processing statistics
  private val processingStats = mutable.Map[String, Long]()

  // Cached Person RDD to avoid repeated generation
  private lazy val cachedPersonRDD = {
    logger.info("Generating and caching Person RDD (with knows relationships)")
    val ldbcConfig = createLdbcConfiguration()

    import ldbc.snb.datagen.generator.generators._
    import ldbc.snb.datagen.generator.DatagenParams
    import ldbc.snb.datagen.entities.Keys._

    DatagenParams.readConf(ldbcConfig)
    Dictionaries.loadDictionaries()

    // Step 1: Generate Person objects
    logger.info("Step 1: Generating Person objects")
    val personRDD = SparkPersonGenerator(ldbcConfig)(spark)

    // Step 2: Generate 3 types of knows relationships (45% university + 45% interest + 10% random)
    logger.info(
      "Step 2: Generating knows relationships (45% university + 45% interest + 10% random)"
    )
    val percentages = Seq(0.45f, 0.45f, 0.1f)
    val knowsGeneratorClassName = DatagenParams.getKnowsGenerator

    val uniRanker = SparkRanker.create(_.byUni)
    val interestRanker = SparkRanker.create(_.byInterest)
    val randomRanker = SparkRanker.create(_.byRandomId)

    logger.info("  - Generating university-based knows relationships")
    val uniKnows = SparkKnowsGenerator(
      personRDD,
      uniRanker,
      ldbcConfig,
      percentages,
      0,
      knowsGeneratorClassName
    )
    logger.info("  - Generating interest-based knows relationships")
    val interestKnows = SparkKnowsGenerator(
      personRDD,
      interestRanker,
      ldbcConfig,
      percentages,
      1,
      knowsGeneratorClassName
    )
    logger.info("  - Generating random knows relationships")
    val randomKnows = SparkKnowsGenerator(
      personRDD,
      randomRanker,
      ldbcConfig,
      percentages,
      2,
      knowsGeneratorClassName
    )

    // Step 3: Merge all knows relationships
    logger.info("Step 3: Merging all knows relationships")
    val personRDDWithKnows =
      SparkKnowsMerger(uniKnows, interestKnows, randomKnows)

    import org.apache.spark.storage.StorageLevel
    val cached = personRDDWithKnows.persist(StorageLevel.MEMORY_AND_DISK_SER)

    logger.info(
      s"Person RDD (with knows relationships) caching completed, storage level: MEMORY_AND_DISK_SER"
    )
    cached
  }

  logger.info("PersonRDDProcessor initialized")

  /**
   * Process Person entity data
   *
   * @return
   *   Try wrapping DataFrame with Person entity data including assigned IDs
   */
  def processPersonFromRDD(): Try[DataFrame] = {
    Try {
      logger.info("Processing Person RDD data")
      val personDF = generatePersonDataFrame()
      val personWithIds = idManager.assignVertexIds(personDF, "Person")

      val recordCount = personWithIds.count()
      processingStats("person_count") = recordCount
      logger.info(s"Person processing completed: record count=$recordCount")

      personWithIds
    }.recover { case e: Exception =>
      logger.error("Person processing failed", e)
      throw e
    }
  }

  /**
   * Process Person relationship data
   *
   * Processes all Person-related relationships including knows, hasInterest,
   * studyAt, workAt, and isLocatedIn.
   *
   * @return
   *   Try wrapping Map of relationship type to DataFrame
   */
  def processPersonRelationsFromRDD(): Try[Map[String, DataFrame]] = {
    Try {
      logger.info("Processing Person relationship data")
      val relations = mutable.Map[String, DataFrame]()

      // Process knows relationships
      val knowsDF = processKnowsRelation()
      relations("knows") = knowsDF
      processingStats("knows_count") = knowsDF.count()

      // Process hasInterest relationships
      val hasInterestDF = processHasInterestRelation()
      relations("hasInterest") = hasInterestDF
      processingStats("hasInterest_count") = hasInterestDF.count()

      // Process studyAt relationships
      val studyAtDF = processStudyAtRelation()
      relations("studyAt") = studyAtDF
      processingStats("studyAt_count") = studyAtDF.count()

      // Process workAt relationships
      val workAtDF = processWorkAtRelation()
      relations("workAt") = workAtDF
      processingStats("workAt_count") = workAtDF.count()

      // Process Person_isLocatedIn_Place relationships
      val isLocatedInDF = processPersonIsLocatedInPlace()
      relations("isLocatedIn") = isLocatedInDF
      processingStats("isLocatedIn_count") = isLocatedInDF.count()

      logger.info(
        s"Person relationship processing completed: knows=${processingStats("knows_count")}, " +
          s"hasInterest=${processingStats("hasInterest_count")}, " +
          s"studyAt=${processingStats("studyAt_count")}, " +
          s"workAt=${processingStats("workAt_count")}, " +
          s"isLocatedIn=${processingStats("isLocatedIn_count")}"
      )

      relations.toMap
    }.recover { case e: Exception =>
      logger.error("Person relationship processing failed", e)
      throw e
    }
  }

  /**
   * Process Person data and add to data collector
   *
   * Processes both Person entities and all related relationships, then adds
   * them to the data collector.
   *
   * @param dataCollector
   *   GraphArDataCollector to store processed data
   * @return
   *   Try wrapping PersonProcessingResult with statistics
   */
  def processAndCollect(
      dataCollector: GraphArDataCollector
  ): Try[PersonProcessingResult] = {
    Try {
      logger.info(
        "Processing Person data and collecting to GraphArDataCollector"
      )

      // Process Person entities
      val personDF = processPersonFromRDD().get
      logger.info(
        s"Person added to DataCollector, record count: ${personDF.count()}"
      )
      dataCollector.addStaticVertexData("Person", personDF)

      // Process Person relationships
      val relations = processPersonRelationsFromRDD().get
      logger.info(
        s"Processing Person relationships, types count: ${relations.size}"
      )

      relations.foreach { case (relationType, df) =>
        val count = df.count()
        logger.info(
          s"Processing relationship $relationType, record count: $count"
        )
        relationType match {
          case "knows" =>
            dataCollector.addStaticEdgeData(("Person", "knows", "Person"), df)
          case "hasInterest" =>
            dataCollector.addStaticEdgeData(
              ("Person", "hasInterest", "Tag"),
              df
            )
          case "studyAt" =>
            dataCollector.addStaticEdgeData(
              ("Person", "studyAt", "Organisation"),
              df
            )
          case "workAt" =>
            dataCollector.addStaticEdgeData(
              ("Person", "workAt", "Organisation"),
              df
            )
          case "isLocatedIn" =>
            dataCollector.addStaticEdgeData(
              ("Person", "isLocatedIn", "Place"),
              df
            )
          case _ => logger.warn(s"Unknown relationship type: $relationType")
        }
      }

      val result = PersonProcessingResult(
        personCount = processingStats("person_count"),
        knowsCount = processingStats("knows_count"),
        hasInterestCount = processingStats("hasInterest_count"),
        studyAtCount = processingStats("studyAt_count"),
        workAtCount = processingStats("workAt_count"),
        isLocatedInCount = processingStats.getOrElse("isLocatedIn_count", 0L),
        success = true
      )

      logger.info(s"Person processing and collection completed: $result")
      result
    }.recover { case e: Exception =>
      logger.error("Person processing and collection failed", e)
      PersonProcessingResult(
        0,
        0,
        0,
        0,
        0,
        success = false,
        error = Some(e.getMessage)
      )
    }
  }

  /** Generate Person DataFrame from cached RDD */
  private def generatePersonDataFrame(): DataFrame = {
    logger.info("Generating Person DataFrame from cached RDD")
    import scala.collection.JavaConverters._
    val personDF = spark.createDataFrame(
      cachedPersonRDD.map { person =>
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
      },
      personSchema
    )

    logger.info("Person DataFrame generated successfully")
    personDF
  }

  /** Create LDBC configuration */
  private def createLdbcConfiguration()
      : ldbc.snb.datagen.util.GeneratorConfiguration = {
    import ldbc.snb.datagen.util.{GeneratorConfiguration, ConfigParser}
    import scala.collection.JavaConverters._

    val scaleFactor =
      spark.conf.getOption("ldbc.scale.factor").map(_.toDouble).getOrElse(0.003)
    // Convert double to integer string for LDBC (e.g., 1.0 -> "1", 0.003 -> "0.003")
    val scaleFactorString =
      if (scaleFactor == scaleFactor.toInt) scaleFactor.toInt.toString
      else scaleFactor.toString
    val config = org.apache.graphar.datasources.ldbc.util.LdbcConfigUtils
      .createFastTestConfig(scaleFactorString)

    logger.info(
      s"LDBC configuration created: scaleFactor=$scaleFactor, scaleFactorString=$scaleFactorString"
    )
    config
  }

  /** Process knows relationships (using LDBC Person.getKnows()) */
  private def processKnowsRelation(): DataFrame = {
    logger.info("Generating knows relationship data")
    val knowsSchema = StructType(
      Array(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("creationDate", TimestampType, nullable = false),
        StructField("weight", DoubleType, nullable = true)
      )
    )

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
    logger.info(s"Generated knows relationships: ${knowsDF.count()} records")
    knowsDF
  }

  /** Process hasInterest relationships (using LDBC Person.getInterests()) */
  private def processHasInterestRelation(): DataFrame = {
    logger.info("Generating hasInterest relationship data")
    val hasInterestSchema = StructType(
      Array(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("creationDate", TimestampType, nullable = false)
      )
    )

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

    val hasInterestDF =
      spark.createDataFrame(hasInterestRows, hasInterestSchema).coalesce(1)
    logger.info(
      s"Generated hasInterest relationships: ${hasInterestDF.count()} records"
    )
    hasInterestDF
  }

  /** Process studyAt relationships (using LDBC official decoding logic) */
  private def processStudyAtRelation(): DataFrame = {
    logger.info("Generating studyAt relationship data")
    val studyAtSchema = StructType(
      Array(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("classYear", IntegerType, nullable = false)
      )
    )

    val studyAtRows = cachedPersonRDD.flatMap { person =>
      val personId = person.getAccountId().toLong
      val universityLocationId = person.getUniversityLocationId()
      val classYear = person.getClassYear()
      val universityId = Dictionaries.universities.getUniversityFromLocation(
        universityLocationId
      )

      if (universityId != -1 && classYear != -1) {
        Some(org.apache.spark.sql.Row(personId, universityId, classYear.toInt))
      } else None
    }

    val studyAtDF =
      spark.createDataFrame(studyAtRows, studyAtSchema).coalesce(1)
    logger.info(
      s"Generated studyAt relationships: ${studyAtDF.count()} records"
    )
    studyAtDF
  }

  /** Process workAt relationships (using LDBC Person.getCompanies()) */
  private def processWorkAtRelation(): DataFrame = {
    logger.info("Generating workAt relationship data")
    val workAtSchema = StructType(
      Array(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false),
        StructField("workFrom", StringType, nullable = false)
      )
    )

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
    logger.info(s"Generated workAt relationships: ${workAtDF.count()} records")
    workAtDF
  }

  /**
   * Process Person_isLocatedIn_Place relationships (using Person.getCityId())
   */
  private def processPersonIsLocatedInPlace(): DataFrame = {
    logger.info("Generating Person_isLocatedIn_Place relationship data")
    val isLocatedInSchema = StructType(
      Array(
        StructField("src", LongType, nullable = false),
        StructField("dst", LongType, nullable = false)
      )
    )

    val isLocatedInRows = cachedPersonRDD.map { person =>
      val personId = person.getAccountId().toLong
      val cityId = person.getCityId().toLong
      org.apache.spark.sql.Row(personId, cityId)
    }

    val isLocatedInDF =
      spark.createDataFrame(isLocatedInRows, isLocatedInSchema).coalesce(1)
    logger.info(
      s"Generated Person_isLocatedIn_Place relationships: ${isLocatedInDF.count()} records"
    )
    isLocatedInDF
  }

  /**
   * Get processing statistics
   *
   * @return
   *   Map of metric name to count
   */
  def getProcessingStatistics(): Map[String, Long] = processingStats.toMap

  /**
   * Validate Person data integrity
   *
   * Validates required fields, ID uniqueness, and NULL value constraints.
   *
   * @param personDF
   *   DataFrame containing Person data to validate
   * @return
   *   ValidationResult indicating success or listing validation errors
   */
  def validatePersonData(
      personDF: DataFrame
  ): org.apache.graphar.datasources.ldbc.model.ValidationResult = {
    try {
      val validationErrors = mutable.ListBuffer[String]()
      val requiredFields = Set("id", "firstName", "lastName", "creationDate")
      val actualFields = personDF.columns.toSet

      // Check required fields
      requiredFields.foreach { field =>
        if (!actualFields.contains(field))
          validationErrors += s"Missing field: $field"
      }

      // Check ID uniqueness
      val totalCount = personDF.count()
      val uniqueIdCount = personDF.select("id").distinct().count()
      if (totalCount != uniqueIdCount) {
        validationErrors += s"ID not unique: total=$totalCount, unique=$uniqueIdCount"
      }

      // Check NULL values
      requiredFields.foreach { field =>
        if (actualFields.contains(field)) {
          val nullCount = personDF.filter(col(field).isNull).count()
          if (nullCount > 0)
            validationErrors += s"Field $field has $nullCount NULL values"
        }
      }

      if (validationErrors.isEmpty) ValidationSuccess
      else ValidationFailure(validationErrors.toList)
    } catch {
      case e: Exception =>
        logger.error("Data validation failed", e)
        ValidationFailure(List(s"Validation error: ${e.getMessage}"))
    }
  }
}

/** Person processing result wrapper */
case class PersonProcessingResult(
    personCount: Long,
    knowsCount: Long,
    hasInterestCount: Long,
    studyAtCount: Long,
    workAtCount: Long,
    isLocatedInCount: Long = 0L,
    success: Boolean,
    error: Option[String] = None
)
