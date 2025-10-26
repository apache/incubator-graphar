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

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.generator.dictionary.Dictionaries
import ldbc.snb.datagen.io.raw.RecordOutputStream
import org.apache.graphar.datasources.ldbc.stream.core.{EntitySchemaDetector}
import org.apache.graphar.datasources.ldbc.stream.processor.PropertyGroupManager
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import java.util.{List => JList}
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * GraphAr Personoutput stream
 *
 * extends LDBC RecordOutputStream[Person]Interface,Aligned
 * withPersonoutputStreamfeature Responsible for converting Person entity and
 * its related relationships to GraphAr format output
 */
class GraphArPersonOutputStream(
    private val output_path: String,
    private val graph_name: String,
    private val file_type: String = "parquet"
)(@transient implicit val spark: SparkSession)
    extends RecordOutputStream[Person]
    with Serializable {

  @transient private val logger: Logger =
    LoggerFactory.getLogger(classOf[GraphArPersonOutputStream])
  private val propertyGroupManager = new PropertyGroupManager()

  // Lazy initialize SparkSession to process deserialization
  @transient private lazy val sparkSession: SparkSession = {
    if (spark != null) spark else SparkSession.active
  }

  // Person entity data collector
  private val personRecords = mutable.ListBuffer[Array[String]]()

  // Relationship data collectors
  private val knowsRecords = mutable.ListBuffer[Array[String]]()
  private val interestRecords = mutable.ListBuffer[Array[String]]()
  private val studyAtRecords = mutable.ListBuffer[Array[String]]()
  private val workAtRecords = mutable.ListBuffer[Array[String]]()

  // Statistics
  private var personCount = 0L
  private var knowsCount = 0L
  private var interestCount = 0L
  private var studyAtCount = 0L
  private var workAtCount = 0L

  logger.info(
    s"GraphArPersonOutputStream initialized: output_path=$output_path, graph_name=$graph_name"
  )

  /**
   * WritePersonEntity - Aligned withPersonoutputStream.write()
   */
  override def write(person: Person): Unit = {
    logger.debug(s"Processing Person: ${person.getAccountId}")

    try {
      // 1. Write Person entity itself
      writePersonEntity(person)

      // 2. WritePerson relationshipData
      writeKnowsRelations(person)
      writeInterestRelations(person)
      writeStudyAtRelations(person)
      writeWorkAtRelations(person)

      personCount += 1

    } catch {
      case e: Exception =>
        logger.error(s"Error processing Person ${person.getAccountId}", e)
        throw e
    }
  }

  /**
   * WritePersonEntityData
   */
  private def writePersonEntity(person: Person): Unit = {
    val personData = Array(
      person.getAccountId.toString,
      person.getFirstName,
      person.getLastName,
      getGender(person.getGender),
      person.getBirthday.toString,
      person.getCreationDate.toString,
      if (person.getDeletionDate != null) person.getDeletionDate.toString
      else "",
      person.isExplicitlyDeleted.toString,
      person.getIpAddress.toString,
      Dictionaries.browsers.getName(person.getBrowserId),
      person.getCityId.toString,
      getLanguages(person.getLanguages),
      getEmails(person.getEmails)
    )

    personRecords += personData
    logger.debug(s"Added Person entity: ${person.getAccountId}")
  }

  /**
   * WriteKnows relationship
   */
  private def writeKnowsRelations(person: Person): Unit = {
    if (person.getKnows != null) {
      person.getKnows.asScala.foreach { knows =>
        val knowsData = Array(
          person.getAccountId.toString,
          knows.to().getAccountId.toString,
          knows.getCreationDate.toString,
          knows.getWeight.toString
        )

        knowsRecords += knowsData
        knowsCount += 1

        logger.debug(
          s"Added knows relationship: ${person.getAccountId} -> ${knows.to().getAccountId}"
        )
      }
    }
  }

  /**
   * WritehasInterestrelationship
   */
  private def writeInterestRelations(person: Person): Unit = {
    if (person.getInterests != null) {
      person.getInterests.asScala.foreach { interestId =>
        val interestData = Array(
          person.getAccountId.toString,
          interestId.toString,
          person.getCreationDate.toString
        )

        interestRecords += interestData
        interestCount += 1

        logger.debug(
          s"Added hasInterest relationship: ${person.getAccountId} -> $interestId"
        )
      }
    }
  }

  /**
   * Write studyAt relationship - fix API call
   */
  private def writeStudyAtRelations(person: Person): Unit = {
    // Aligned withPersonoutputStream.scala:59-69 implement
    val universityId = Dictionaries.universities.getUniversityFromLocation(
      person.getUniversityLocationId
    )
    if (universityId != -1 && person.getClassYear != -1) {
      val studyAtData = Array(
        person.getAccountId.toString,
        universityId.toString,
        person.getClassYear.toString
      )

      studyAtRecords += studyAtData
      studyAtCount += 1

      logger.debug(
        s"Added studyAt relationship: ${person.getAccountId} -> $universityId"
      )
    }
  }

  /**
   * Write workAt relationship - fix API call
   */
  private def writeWorkAtRelations(person: Person): Unit = {
    // Aligned withPersonoutputStream.scala:71-80 implement
    if (person.getCompanies != null) {
      person.getCompanies.asScala.foreach { case (companyId, workFrom) =>
        val workAtData = Array(
          person.getAccountId.toString,
          companyId.toString,
          workFrom.toString
        )

        workAtRecords += workAtData
        workAtCount += 1

        logger.debug(
          s"Added workAt relationship: ${person.getAccountId} -> $companyId"
        )
      }
    }
  }

  /**
   * Auxiliary method: Get gender string
   */
  private def getGender(gender: Int): String =
    if (gender == 0) "female" else "male"

  /**
   * Auxiliary method: Get language list
   */
  private def getLanguages(languages: java.util.List[Integer]): String = {
    if (languages != null) {
      languages.asScala.map(_.toString).mkString(";")
    } else {
      ""
    }
  }

  /**
   * Auxiliary method: Get email list
   */
  private def getEmails(emails: java.util.List[String]): String = {
    if (emails != null) {
      emails.asScala.mkString(";")
    } else {
      ""
    }
  }

  /**
   * FlushDatatoGraphArFormatFile
   */
  def flush(): Unit = {
    logger.info("Starting to flush Person data to GraphAr format")

    try {
      // WritePersonEntity
      if (personRecords.nonEmpty) {
        writePersonToGraphAr()
      }

      // WriterelationshipData
      if (knowsRecords.nonEmpty) {
        writeKnowsToGraphAr()
      }

      if (interestRecords.nonEmpty) {
        writeInterestToGraphAr()
      }

      if (studyAtRecords.nonEmpty) {
        writeStudyAtToGraphAr()
      }

      if (workAtRecords.nonEmpty) {
        writeWorkAtToGraphAr()
      }

      logger.info(
        s"Person data flush completed: Person=${personCount}, knows=${knowsCount}, interest=${interestCount}, studyAt=${studyAtCount}, workAt=${workAtCount}"
      )

    } catch {
      case e: Exception =>
        logger.error("Error flushing Person data", e)
        throw e
    }
  }

  /**
   * WritePersonEntitytoGraphArFormat
   */
  private def writePersonToGraphAr(): Unit = {
    val personHeader = Array(
      "id",
      "firstName",
      "lastName",
      "gender",
      "birthday",
      "creationDate",
      "deletionDate",
      "explicitlyDeleted",
      "locationIP",
      "browserUsed",
      "cityId",
      "languages",
      "emails"
    )

    val personDF = createDataFrame(personRecords.toList, personHeader)
    writeDataFrameToGraphAr(personDF, s"$output_path/vertex/Person")

    logger.info(s"Person entity write completed: ${personRecords.size} records")
  }

  /**
   * WriteKnows relationshiptoGraphArFormat
   */
  private def writeKnowsToGraphAr(): Unit = {
    val knowsHeader = Array("src", "dst", "creationDate", "weight")

    val knowsDF = createDataFrame(knowsRecords.toList, knowsHeader)
    writeDataFrameToGraphAr(knowsDF, s"$output_path/edge/Person_knows_Person")

    logger.info(
      s"Knows relationship write completed: ${knowsRecords.size} records"
    )
  }

  /**
   * WritehasInterestrelationshiptoGraphArFormat
   */
  private def writeInterestToGraphAr(): Unit = {
    val interestHeader = Array("personId", "tagId", "creationDate")

    val interestDF = createDataFrame(interestRecords.toList, interestHeader)
    writeDataFrameToGraphAr(
      interestDF,
      s"$output_path/edge/Person_hasInterest_Tag"
    )

    logger.info(
      s"HasInterest relationship write completed: ${interestRecords.size} records"
    )
  }

  /**
   * WritestudyAtrelationshiptoGraphArFormat
   */
  private def writeStudyAtToGraphAr(): Unit = {
    val studyAtHeader = Array("personId", "universityId", "classYear")

    val studyAtDF = createDataFrame(studyAtRecords.toList, studyAtHeader)
    writeDataFrameToGraphAr(
      studyAtDF,
      s"$output_path/edge/Person_studyAt_Organisation"
    )

    logger.info(
      s"StudyAt relationship write completed: ${studyAtRecords.size} records"
    )
  }

  /**
   * WriteworkAtrelationshiptoGraphArFormat
   */
  private def writeWorkAtToGraphAr(): Unit = {
    val workAtHeader = Array("personId", "companyId", "workFrom")

    val workAtDF = createDataFrame(workAtRecords.toList, workAtHeader)
    writeDataFrameToGraphAr(
      workAtDF,
      s"$output_path/edge/Person_workAt_Organisation"
    )

    logger.info(
      s"WorkAt relationship write completed: ${workAtRecords.size} records"
    )
  }

  /**
   * CreateDataFrame
   */
  private def createDataFrame(
      records: List[Array[String]],
      header: Array[String]
  ): DataFrame = {
    val rows = records.map(record => Row.fromSeq(record.toSeq))
    val schema = StructType(
      header.map(field => StructField(field, StringType, nullable = true))
    )

    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(rows),
      schema
    )
  }

  /**
   * WriteDataFrametoGraphArFormat
   */
  private def writeDataFrameToGraphAr(
      df: DataFrame,
      outputDir: String
  ): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .format(file_type)
      .save(outputDir)
  }

  /**
   * Closeoutput stream
   */
  override def close(): Unit = {
    logger.info("Closing GraphArPersonOutputStream")

    try {
      flush()
    } catch {
      case e: Exception =>
        logger.error("Error flushing data during close", e)
    }

    // Clean upMemory
    personRecords.clear()
    knowsRecords.clear()
    interestRecords.clear()
    studyAtRecords.clear()
    workAtRecords.clear()
  }

  /**
   * GetStatistics
   */
  def getStatistics(): Map[String, Long] = {
    Map(
      "personCount" -> personCount,
      "knowsCount" -> knowsCount,
      "interestCount" -> interestCount,
      "studyAtCount" -> studyAtCount,
      "workAtCount" -> workAtCount
    )
  }

  /**
   * useSchemaSupport
   */
  def use[T](f: GraphArPersonOutputStream => T): T = {
    try {
      f(this)
    } finally {
      close()
    }
  }
}
