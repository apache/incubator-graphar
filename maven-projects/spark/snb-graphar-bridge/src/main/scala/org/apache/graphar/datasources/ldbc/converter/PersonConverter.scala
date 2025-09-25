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

package org.apache.graphar.datasources.ldbc.converter

import ldbc.snb.datagen.entities.dynamic.person.Person
import org.apache.graphar.datasources.ldbc.model.{PersonData, ValidationResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
 * Person converter implementation
 * Converts LDBC Person entities to GraphAr format
 */
class PersonConverter extends LdbcDataConverter[Person, PersonData] {

  override def convert(rdd: RDD[Person])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Step 1: Convert to intermediate data structure
    val personDataRDD = rdd.map(convertPerson)

    // Step 2: Convert to DataFrame
    val df = personDataRDD.toDF()

    // Step 3: Generate GraphAr required consecutive IDs
    val windowSpec = Window.orderBy("originalId")
    df.withColumn("id", row_number().over(windowSpec) - 1)
      .select(getGraphArSchema().fieldNames.map(col): _*)
  }

  private def convertPerson(person: Person): PersonData = {
    PersonData(
      id = -1L, // Will be assigned by window function
      originalId = person.getAccountId,
      firstName = Option(person.getFirstName).getOrElse(""),
      lastName = Option(person.getLastName).getOrElse(""),
      birthday = person.getBirthday,
      creationDate = person.getCreationDate,
      gender = if (person.getGender == 1) "male" else "female",
      browserUsed = person.getBrowserId.toString,
      locationIp = Option(person.getIpAddress).map(_.toString).getOrElse(""),
      cityId = person.getCityId,
      languages = Option(person.getLanguages)
        .map(_.asScala.mkString(";"))
        .getOrElse(""),
      emails = Option(person.getEmails)
        .map(_.asScala.mkString(";"))
        .getOrElse("")
    )
  }

  override def getGraphArSchema(): StructType = {
    StructType(Seq(
      StructField("id", LongType, nullable = false),           // GraphAr consecutive ID
      StructField("originalId", LongType, nullable = false),   // LDBC original ID
      StructField("firstName", StringType, nullable = false),
      StructField("lastName", StringType, nullable = false),
      StructField("birthday", LongType, nullable = false),
      StructField("creationDate", LongType, nullable = false),
      StructField("gender", StringType, nullable = false),
      StructField("browserUsed", StringType, nullable = false),
      StructField("locationIp", StringType, nullable = false),
      StructField("cityId", IntegerType, nullable = false),
      StructField("languages", StringType, nullable = true),
      StructField("emails", StringType, nullable = true)
    ))
  }

  override def validateConversion(original: RDD[Person], converted: DataFrame): ValidationResult = {
    val basicValidation = super.validateConversion(original, converted)

    if (!basicValidation.isSuccess) {
      return basicValidation
    }

    // Additional Person-specific validations
    val idValidation = validateConsecutiveIds(converted)

    ValidationResult.combine(basicValidation, idValidation)
  }

  private def validateConsecutiveIds(df: DataFrame): ValidationResult = {
    val ids = df.select("id").rdd.map(_.getLong(0)).collect().sorted
    val expectedIds = (0L until ids.length).toArray

    if (ids.sameElements(expectedIds)) {
      ValidationResult.success()
    } else {
      ValidationResult.failure("Person IDs must be consecutive starting from 0")
    }
  }
}