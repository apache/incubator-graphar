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
 * GraphAr Person输出流
 *
 * 继承LDBC的RecordOutputStream[Person]接口，对标PersonOutputStream功能
 * 负责将Person实体及其相关关系转换为GraphAr格式输出
 */
class GraphArPersonOutputStream(
  private val outputPath: String,
  private val graphName: String,
  private val fileType: String = "parquet"
)(@transient implicit val spark: SparkSession) extends RecordOutputStream[Person] with Serializable {

  @transient private val logger: Logger = LoggerFactory.getLogger(classOf[GraphArPersonOutputStream])
  private val propertyGroupManager = new PropertyGroupManager()

  // 懒初始化SparkSession以处理反序列化
  @transient private lazy val sparkSession: SparkSession = {
    if (spark != null) spark else SparkSession.active
  }

  // Person实体数据收集器
  private val personRecords = mutable.ListBuffer[Array[String]]()

  // 关系数据收集器
  private val knowsRecords = mutable.ListBuffer[Array[String]]()
  private val interestRecords = mutable.ListBuffer[Array[String]]()
  private val studyAtRecords = mutable.ListBuffer[Array[String]]()
  private val workAtRecords = mutable.ListBuffer[Array[String]]()

  // 统计信息
  private var personCount = 0L
  private var knowsCount = 0L
  private var interestCount = 0L
  private var studyAtCount = 0L
  private var workAtCount = 0L

  logger.info(s"GraphArPersonOutputStream初始化: outputPath=$outputPath, graphName=$graphName")

  /**
   * 写入Person实体 - 对标PersonOutputStream.write()
   */
  override def write(person: Person): Unit = {
    logger.debug(s"处理Person: ${person.getAccountId}")

    try {
      // 1. 写入Person实体本身
      writePersonEntity(person)

      // 2. 写入Person的关系数据
      writeKnowsRelations(person)
      writeInterestRelations(person)
      writeStudyAtRelations(person)
      writeWorkAtRelations(person)

      personCount += 1

    } catch {
      case e: Exception =>
        logger.error(s"处理Person ${person.getAccountId} 时出错", e)
        throw e
    }
  }

  /**
   * 写入Person实体数据
   */
  private def writePersonEntity(person: Person): Unit = {
    val personData = Array(
      person.getAccountId.toString,
      person.getFirstName,
      person.getLastName,
      getGender(person.getGender),
      person.getBirthday.toString,
      person.getCreationDate.toString,
      if (person.getDeletionDate != null) person.getDeletionDate.toString else "",
      person.isExplicitlyDeleted.toString,
      person.getIpAddress.toString,
      Dictionaries.browsers.getName(person.getBrowserId),
      person.getCityId.toString,
      getLanguages(person.getLanguages),
      getEmails(person.getEmails)
    )

    personRecords += personData
    logger.debug(s"添加Person实体: ${person.getAccountId}")
  }

  /**
   * 写入knows关系
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

        logger.debug(s"添加knows关系: ${person.getAccountId} -> ${knows.to().getAccountId}")
      }
    }
  }

  /**
   * 写入hasInterest关系
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

        logger.debug(s"添加hasInterest关系: ${person.getAccountId} -> $interestId")
      }
    }
  }

  /**
   * 写入studyAt关系 - 修复API调用
   */
  private def writeStudyAtRelations(person: Person): Unit = {
    // 对标PersonOutputStream.scala:59-69的实现
    val universityId = Dictionaries.universities.getUniversityFromLocation(person.getUniversityLocationId)
    if (universityId != -1 && person.getClassYear != -1) {
      val studyAtData = Array(
        person.getAccountId.toString,
        universityId.toString,
        person.getClassYear.toString
      )

      studyAtRecords += studyAtData
      studyAtCount += 1

      logger.debug(s"添加studyAt关系: ${person.getAccountId} -> $universityId")
    }
  }

  /**
   * 写入workAt关系 - 修复API调用
   */
  private def writeWorkAtRelations(person: Person): Unit = {
    // 对标PersonOutputStream.scala:71-80的实现
    if (person.getCompanies != null) {
      person.getCompanies.asScala.foreach { case (companyId, workFrom) =>
        val workAtData = Array(
          person.getAccountId.toString,
          companyId.toString,
          workFrom.toString
        )

        workAtRecords += workAtData
        workAtCount += 1

        logger.debug(s"添加workAt关系: ${person.getAccountId} -> $companyId")
      }
    }
  }

  /**
   * 辅助方法：获取性别字符串
   */
  private def getGender(gender: Int): String = if (gender == 0) "female" else "male"

  /**
   * 辅助方法：获取语言列表
   */
  private def getLanguages(languages: java.util.List[Integer]): String = {
    if (languages != null) {
      languages.asScala.map(x => Dictionaries.languages.getLanguageName(x)).mkString(";")
    } else {
      ""
    }
  }

  /**
   * 辅助方法：获取邮箱列表
   */
  private def getEmails(emails: java.util.List[String]): String = {
    if (emails != null) {
      emails.asScala.mkString(";")
    } else {
      ""
    }
  }

  /**
   * 刷新数据到GraphAr格式文件
   */
  def flush(): Unit = {
    logger.info("开始刷新Person数据到GraphAr格式")

    try {
      // 写入Person实体
      if (personRecords.nonEmpty) {
        writePersonToGraphAr()
      }

      // 写入关系数据
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

      logger.info(s"Person数据刷新完成: Person=${personCount}, knows=${knowsCount}, interest=${interestCount}, studyAt=${studyAtCount}, workAt=${workAtCount}")

    } catch {
      case e: Exception =>
        logger.error("刷新Person数据时出错", e)
        throw e
    }
  }

  /**
   * 写入Person实体到GraphAr格式
   */
  private def writePersonToGraphAr(): Unit = {
    val personHeader = Array("id", "firstName", "lastName", "gender", "birthday",
                            "creationDate", "deletionDate", "explicitlyDeleted",
                            "locationIP", "browserUsed", "cityId", "languages", "emails")

    val personDF = createDataFrame(personRecords.toList, personHeader)
    writeDataFrameToGraphAr(personDF, s"$outputPath/vertex/Person")

    logger.info(s"写入Person实体完成: ${personRecords.size}条记录")
  }

  /**
   * 写入knows关系到GraphAr格式
   */
  private def writeKnowsToGraphAr(): Unit = {
    val knowsHeader = Array("src", "dst", "creationDate", "weight")

    val knowsDF = createDataFrame(knowsRecords.toList, knowsHeader)
    writeDataFrameToGraphAr(knowsDF, s"$outputPath/edge/Person_knows_Person")

    logger.info(s"写入knows关系完成: ${knowsRecords.size}条记录")
  }

  /**
   * 写入hasInterest关系到GraphAr格式
   */
  private def writeInterestToGraphAr(): Unit = {
    val interestHeader = Array("personId", "tagId", "creationDate")

    val interestDF = createDataFrame(interestRecords.toList, interestHeader)
    writeDataFrameToGraphAr(interestDF, s"$outputPath/edge/Person_hasInterest_Tag")

    logger.info(s"写入hasInterest关系完成: ${interestRecords.size}条记录")
  }

  /**
   * 写入studyAt关系到GraphAr格式
   */
  private def writeStudyAtToGraphAr(): Unit = {
    val studyAtHeader = Array("personId", "universityId", "classYear")

    val studyAtDF = createDataFrame(studyAtRecords.toList, studyAtHeader)
    writeDataFrameToGraphAr(studyAtDF, s"$outputPath/edge/Person_studyAt_Organisation")

    logger.info(s"写入studyAt关系完成: ${studyAtRecords.size}条记录")
  }

  /**
   * 写入workAt关系到GraphAr格式
   */
  private def writeWorkAtToGraphAr(): Unit = {
    val workAtHeader = Array("personId", "companyId", "workFrom")

    val workAtDF = createDataFrame(workAtRecords.toList, workAtHeader)
    writeDataFrameToGraphAr(workAtDF, s"$outputPath/edge/Person_workAt_Organisation")

    logger.info(s"写入workAt关系完成: ${workAtRecords.size}条记录")
  }

  /**
   * 创建DataFrame
   */
  private def createDataFrame(records: List[Array[String]], header: Array[String]): DataFrame = {
    val rows = records.map(record => Row.fromSeq(record.toSeq))
    val schema = StructType(header.map(field => StructField(field, StringType, nullable = true)))

    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows), schema)
  }

  /**
   * 写入DataFrame到GraphAr格式
   */
  private def writeDataFrameToGraphAr(df: DataFrame, outputDir: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .format(fileType)
      .save(outputDir)
  }

  /**
   * 关闭输出流
   */
  override def close(): Unit = {
    logger.info("关闭GraphArPersonOutputStream")

    try {
      flush()
    } catch {
      case e: Exception =>
        logger.error("关闭时刷新数据失败", e)
    }

    // 清理内存
    personRecords.clear()
    knowsRecords.clear()
    interestRecords.clear()
    studyAtRecords.clear()
    workAtRecords.clear()
  }

  /**
   * 获取统计信息
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
   * use模式支持
   */
  def use[T](f: GraphArPersonOutputStream => T): T = {
    try {
      f(this)
    } finally {
      close()
    }
  }
}