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

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import ldbc.snb.datagen.generator.dictionary.Dictionaries
import ldbc.snb.datagen.generator.vocabulary.DBP
import ldbc.snb.datagen.entities.statictype.place.Place
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Static Entity Processor
 *
 * Responsible for generating static entity DataFrames from LDBC Dictionaries,
 * including Organisation, Place, Tag, TagClass, etc.
 *
 * Design rationale:
 *   1. Reuse LDBC Dictionaries interface to obtain static data 2. Return
 *      DataFrame compatible with GraphWriter 3. Use UnifiedIdManager to ensure
 *      ID space coordination
 */
class StaticEntityProcessor(
    idManager: UnifiedIdManager
)(implicit spark: SparkSession)
    extends Serializable {

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[StaticEntityProcessor])

  /**
   * Initialize LDBC Dictionaries with proper configuration
   */
  private def initializeDictionaries(): Unit = {
    try {
      import ldbc.snb.datagen.generator.DatagenParams
      import ldbc.snb.datagen.util.ConfigParser

      // Create LDBC configuration
      val scaleFactor = spark.conf
        .getOption("ldbc.scale.factor")
        .map(_.toDouble)
        .getOrElse(0.003)
      val config = org.apache.graphar.datasources.ldbc.util.LdbcConfigUtils
        .createFastTestConfig(scaleFactor.toString)

      // Initialize DatagenParams and Dictionaries
      DatagenParams.readConf(config)
      Dictionaries.loadDictionaries() // No parameters needed

      logger.info("LDBC Dictionaries initialized successfully")
    } catch {
      case e: Exception =>
        logger.error("LDBC Dictionaries initialization failed", e)
        throw e
    }
  }

  /**
   * Generate DataFrames for all static entities
   *
   * @return
   *   Map of entity type name to DataFrame
   * @throws RuntimeException
   *   if LDBC Dictionaries cannot be initialized
   */
  def generateAllStaticEntities(): Map[String, DataFrame] = {
    logger.info("Starting to generate all static entity data")

    // Always try to initialize LDBC dictionaries for real data
    val dictionariesAvailable = Try {
      if (!validateDictionaries()) {
        logger.info("Initializing LDBC dictionaries...")
        initializeDictionaries()
      }
      validateDictionaries()
    }.getOrElse(false)

    if (!dictionariesAvailable) {
      logger.error(
        "Cannot initialize LDBC dictionaries, unable to continue generating real data"
      )
      throw new RuntimeException(
        "LDBC Dictionaries initialization failed - cannot generate real data"
      )
    }

    // Only use real LDBC data, no fallback to simulated data
    val entities = Map(
      "Organisation" -> generateOrganisationDF(),
      "Place" -> generatePlaceDF(),
      "Tag" -> generateTagDF(),
      "TagClass" -> generateTagClassDF()
    )

    entities.foreach { case (entityType, df) =>
      val count = df.count()
      if (count > 0) {
        logger.info(s"Generated static entity $entityType: $count records")
      } else {
        logger.warn(s"Static entity $entityType is empty")
      }
    }

    entities
  }

  /**
   * Generate Organisation DataFrame Contains both Company and University types
   */
  private def generateOrganisationDF(): DataFrame = {
    import spark.implicits._

    logger.info("Generating Organisation entity data")

    // Process company data
    val companies =
      if (
        Dictionaries.companies != null && Dictionaries.companies.getCompanies != null
      ) {
        val companyIds =
          Dictionaries.companies.getCompanies.iterator().asScala.toList
        logger.info(s"Found ${companyIds.size} companies")

        companyIds.map { companyId =>
          val name = Option(Dictionaries.companies.getCompanyName(companyId))
            .getOrElse(s"Company_$companyId")
          val countryId = Dictionaries.companies.getCountry(companyId)

          Row(
            companyId.toLong,
            "Company",
            name,
            DBP.getUrl(name),
            countryId.toLong
          )
        }
      } else {
        logger.warn("Companies dictionary not initialized or empty")
        List.empty[Row]
      }

    // Process university data
    val universities =
      if (
        Dictionaries.universities != null && Dictionaries.universities.getUniversities != null
      ) {
        val universityIds =
          Dictionaries.universities.getUniversities.iterator().asScala.toList
        logger.info(s"Found ${universityIds.size} universities")

        universityIds.map { universityId =>
          val name = Option(
            Dictionaries.universities.getUniversityName(universityId)
          ).getOrElse(s"University_$universityId")
          val cityId = Dictionaries.universities.getUniversityCity(universityId)

          Row(
            universityId.toLong, // ✓ Use original universityId (distinguished by type field)
            "University",
            name,
            DBP.getUrl(name),
            cityId.toLong
          )
        }
      } else {
        logger.warn("Universities dictionary not initialized or empty")
        List.empty[Row]
      }

    // Merge data
    val allOrganisations = companies ++ universities

    if (allOrganisations.isEmpty) {
      logger.error(
        "No Organisation data found, LDBC dictionaries may not be properly initialized"
      )
      throw new RuntimeException(
        "No Organisation data found - LDBC Dictionaries may not be properly initialized"
      )
    }

    val rdd = spark.sparkContext.parallelize(allOrganisations)
    val df = spark.createDataFrame(rdd, organisationSchema())
    idManager.assignVertexIds(df, "Organisation")
  }

  /**
   * Generate Place DataFrame
   */
  private def generatePlaceDF(): DataFrame = {
    import spark.implicits._

    logger.info("Generating Place entity data")

    if (Dictionaries.places == null || Dictionaries.places.getPlaces == null) {
      logger.error("Places dictionary not initialized")
      throw new RuntimeException(
        "Places dictionary not initialized - cannot generate Place data"
      )
    }

    val placeIds = Dictionaries.places.getPlaces.iterator().asScala.toList
    logger.info(s"Found ${placeIds.size} places")

    val places = placeIds.flatMap { placeId =>
      Try {
        val place = Dictionaries.places.getLocation(placeId)
        if (place != null) {
          val placeType = place.getType match {
            case Place.CITY      => "City"
            case Place.COUNTRY   => "Country"
            case Place.CONTINENT => "Continent"
            case _               => "Unknown"
          }

          val parentId = if (place.getType != Place.CONTINENT) {
            val parent = Dictionaries.places.belongsTo(placeId)
            if (parent != -1) parent.toLong else null
          } else null

          Some(
            Row(
              placeId.toLong,
              place.getName,
              DBP.getUrl(place.getName),
              placeType,
              parentId
            )
          )
        } else None
      }.getOrElse(None)
    }

    if (places.isEmpty) {
      createEmptyPlaceDF()
    } else {
      val rdd = spark.sparkContext.parallelize(places)
      val df = spark.createDataFrame(rdd, placeSchema())
      idManager.assignVertexIds(df, "Place")
    }
  }

  /**
   * Generate Tag DataFrame
   */
  private def generateTagDF(): DataFrame = {
    import spark.implicits._

    logger.info("Generating Tag entity data")

    if (Dictionaries.tags == null || Dictionaries.tags.getTags == null) {
      logger.error("Tags dictionary not initialized")
      throw new RuntimeException(
        "Tags dictionary not initialized - cannot generate Tag data"
      )
    }

    val tagIds = Dictionaries.tags.getTags.iterator().asScala.toList
    logger.info(s"Found ${tagIds.size} tags")

    val tags = tagIds.map { tagId =>
      val name =
        Option(Dictionaries.tags.getName(tagId)).getOrElse(s"Tag_$tagId")
      val tagClassId = Try(Dictionaries.tags.getTagClass(tagId)).getOrElse(0)

      Row(
        tagId.toLong,
        name,
        DBP.getUrl(name),
        tagClassId
      )
    }

    if (tags.isEmpty) {
      createEmptyTagDF()
    } else {
      val rdd = spark.sparkContext.parallelize(tags)
      val df = spark.createDataFrame(rdd, tagSchema())
      idManager.assignVertexIds(df, "Tag")
    }
  }

  /**
   * Generate TagClass DataFrame
   */
  private def generateTagClassDF(): DataFrame = {
    import spark.implicits._

    logger.info("Generating TagClass entity data (using real LDBC data only)")

    // Only use LDBC TagDictionary API to get real TagClass data, no simulated data
    val tagClasses = if (Dictionaries.tags != null) {
      // Extract tagClass IDs from tags (explicit type annotation ensures type safety)
      val tagClassIds: List[Int] = Try {
        val tags = Dictionaries.tags.getTags
        if (tags != null) {
          tags
            .iterator()
            .asScala
            .map { tagId =>
              Dictionaries.tags.getTagClass(tagId).asInstanceOf[Int]
            }
            .toSet
            .toList
        } else {
          logger.warn(
            "Dictionaries.tags.getTags returned null, cannot get TagClass data"
          )
          List.empty[Int]
        }
      }.getOrElse {
        logger.warn("Failed to extract TagClass IDs")
        List.empty[Int]
      }

      logger.info(s"Found ${tagClassIds.size} tag classes from Tags")

      if (tagClassIds.nonEmpty) {
        tagClassIds.map { tagClassId =>
          // Use LDBC API to get real TagClass data
          val className = Dictionaries.tags.getClassName(tagClassId)
          val classLabel = Dictionaries.tags.getClassLabel(tagClassId)
          val parentId = Dictionaries.tags.getClassParent(tagClassId)

          Row(
            tagClassId,
            className, // Real TagClass name
            s"http://dbpedia.org/resource/$classLabel", // URL based on real label
            if (parentId == -1) null else parentId // Real parent relationship
          )
        }
      } else {
        logger.warn(
          "Failed to get any TagClass data, will return empty DataFrame"
        )
        List.empty[Row]
      }
    } else {
      logger.error(
        "Tags dictionary not initialized, cannot generate TagClass data, returning empty DataFrame"
      )
      List.empty[Row]
    }

    if (tagClasses.isEmpty) {
      createEmptyTagClassDF()
    } else {
      val rdd = spark.sparkContext.parallelize(tagClasses)
      val df = spark.createDataFrame(rdd, tagClassSchema())
      idManager.assignVertexIds(df, "TagClass")
    }
  }

  /**
   * Validate whether LDBC dictionaries are initialized
   */
  private def validateDictionaries(): Boolean = {
    val checks = Map(
      "companies" -> (Dictionaries.companies != null),
      "universities" -> (Dictionaries.universities != null),
      "places" -> (Dictionaries.places != null),
      "tags" -> (Dictionaries.tags != null)
      // tagClasses is not an independent dictionary, inferred from tags
    )

    checks.foreach { case (dict, initialized) =>
      logger.info(s"Dictionary $dict status: ${if (initialized) "initialized"
      else "not initialized"}")
    }

    // Continue as long as some dictionaries are initialized
    checks.values.exists(identity)
  }

  // Schema definitions
  private def organisationSchema(): StructType = StructType(
    Array(
      StructField("id", LongType, nullable = false),
      StructField("type", StringType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("url", StringType, nullable = true),
      StructField("locationId", LongType, nullable = false)
    )
  )

  private def placeSchema(): StructType = StructType(
    Array(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("url", StringType, nullable = true),
      StructField("type", StringType, nullable = false),
      StructField("isPartOf", LongType, nullable = true)
    )
  )

  private def tagSchema(): StructType = StructType(
    Array(
      StructField("id", LongType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("url", StringType, nullable = true),
      StructField("hasType", IntegerType, nullable = false)
    )
  )

  private def tagClassSchema(): StructType = StructType(
    Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("url", StringType, nullable = true),
      StructField("isSubclassOf", IntegerType, nullable = true)
    )
  )

  // Helper methods to create empty DataFrames
  private def createEmptyOrganisationDF(): DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.emptyRDD[Row],
      organisationSchema()
    )
  }

  private def createEmptyPlaceDF(): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], placeSchema())
  }

  private def createEmptyTagDF(): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], tagSchema())
  }

  private def createEmptyTagClassDF(): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], tagClassSchema())
  }

  // ========== Edge generation methods (ExplodeEdges logic) ==========

  /**
   * Generate Organisation_isLocatedIn_Place edges Extracted from
   * Organisation.locationId column
   *
   * @param organisationDF
   *   DataFrame containing Organisation vertices
   * @return
   *   DataFrame with src/dst columns representing edge relationships
   */
  def generateOrgIsLocatedInPlace(organisationDF: DataFrame): DataFrame = {
    import spark.implicits._

    logger.info("Generating Organisation_isLocatedIn_Place edges")

    organisationDF
      .filter(col("locationId").isNotNull)
      .select(
        col("id").as("src"),
        col("locationId").as("dst")
      )
  }

  /**
   * Generate Place_isPartOf_Place edges Extracted from Place.isPartOf column
   * (geographic hierarchy relationship)
   *
   * @param placeDF
   *   DataFrame containing Place vertices
   * @return
   *   DataFrame with src/dst columns representing hierarchical relationships
   */
  def generatePlaceIsPartOfPlace(placeDF: DataFrame): DataFrame = {
    import spark.implicits._

    logger.info("Generating Place_isPartOf_Place edges")

    placeDF
      .filter(col("isPartOf").isNotNull)
      .select(
        col("id").as("src"),
        col("isPartOf").as("dst")
      )
  }

  /**
   * Generate Tag_hasType_TagClass edges Extracted from Tag.hasType column
   *
   * @param tagDF
   *   DataFrame containing Tag vertices
   * @return
   *   DataFrame with src/dst columns linking Tags to TagClasses
   */
  def generateTagHasTypeTagClass(tagDF: DataFrame): DataFrame = {
    import spark.implicits._

    logger.info("Generating Tag_hasType_TagClass edges")

    tagDF
      .filter(col("hasType").isNotNull)
      .select(
        col("id").as("src"),
        col("hasType").cast(LongType).as("dst") // Ensure type consistency
      )
  }

  /**
   * Generate TagClass_isSubclassOf_TagClass edges Extracted from
   * TagClass.isSubclassOf column
   *
   * @param tagClassDF
   *   DataFrame containing TagClass vertices
   * @return
   *   DataFrame with src/dst columns representing TagClass hierarchy
   */
  def generateTagClassIsSubclassOf(tagClassDF: DataFrame): DataFrame = {
    import spark.implicits._

    logger.info("Generating TagClass_isSubclassOf_TagClass edges")

    tagClassDF
      .filter(col("isSubclassOf").isNotNull)
      .select(
        col("id").cast(LongType).as("src"), // Ensure type consistency
        col("isSubclassOf").cast(LongType).as("dst")
      )
  }

  /**
   * Generate all static edge relationships
   *
   * Generates vertices first, then extracts all edge types from vertex
   * attributes.
   *
   * @return
   *   Map of edge type name to DataFrame with src/dst columns
   */
  def generateAllStaticEdges(): Map[String, DataFrame] = {
    logger.info("Starting to generate all static edge relationships")

    // First generate vertex data (needed to extract edges)
    val vertices = generateAllStaticEntities()

    val organisationDF = vertices("Organisation")
    val placeDF = vertices("Place")
    val tagDF = vertices("Tag")
    val tagClassDF = vertices("TagClass")

    // Generate 5 types of edges
    val edges = Map(
      "Organisation_isLocatedIn_Place" -> generateOrgIsLocatedInPlace(
        organisationDF
      ),
      "Place_isPartOf_Place" -> generatePlaceIsPartOfPlace(placeDF),
      "Tag_hasType_TagClass" -> generateTagHasTypeTagClass(tagDF),
      "TagClass_isSubclassOf_TagClass" -> generateTagClassIsSubclassOf(
        tagClassDF
      )
    )

    // Record count for each edge type
    edges.foreach { case (edgeType, df) =>
      val count = df.count()
      logger.info(s"Generated static edge $edgeType: $count records")
    }

    edges
  }
}
