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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.Date
import scala.collection.JavaConverters._
import scala.util.Random

class PersonConverterTest extends AnyFunSuite with Matchers with SparkTestBase {

  test("PersonConverter should convert Person RDD to DataFrame with correct schema") {
    implicit val sparkSession: SparkSession = spark
    val converter = new PersonConverter()
    val personRDD = createTestPersonRDD(10)

    val result = converter.convert(personRDD)

    result.schema shouldEqual converter.getGraphArSchema()
    result.count() shouldEqual 10
  }

  test("PersonConverter should generate consecutive IDs starting from 0") {
    implicit val sparkSession: SparkSession = spark
    val converter = new PersonConverter()
    val personRDD = createTestPersonRDD(5)

    val result = converter.convert(personRDD)
    val ids = result.select("id").collect().map(_.getLong(0)).sorted

    ids shouldEqual Array(0L, 1L, 2L, 3L, 4L)
  }

  test("PersonConverter should preserve original LDBC IDs for mapping") {
    implicit val sparkSession: SparkSession = spark
    val converter = new PersonConverter()
    val originalIds = Array(1001L, 1002L, 1003L)
    val personRDD = createTestPersonRDDWithIds(originalIds)

    val result = converter.convert(personRDD)
    val mappedOriginalIds = result.select("originalId").collect().map(_.getLong(0)).sorted

    mappedOriginalIds shouldEqual originalIds.sorted
  }

  test("PersonConverter should handle null values correctly") {
    implicit val sparkSession: SparkSession = spark
    val converter = new PersonConverter()
    val personRDD = createTestPersonRDDWithNulls()

    val result = converter.convert(personRDD)

    result.count() shouldEqual 3

    val emailCount = result.filter(col("email").isNotNull).count()
    emailCount shouldEqual 2  // Only 2 out of 3 have emails
  }

  test("PersonConverter validation should pass for valid conversions") {
    implicit val sparkSession: SparkSession = spark
    val converter = new PersonConverter()
    val personRDD = createTestPersonRDD(10)
    val result = converter.convert(personRDD)

    val validation = converter.validateConversion(personRDD, result)
    validation.isSuccess shouldBe true
  }

  test("PersonConverter should handle empty RDD") {
    implicit val sparkSession: SparkSession = spark
    val converter = new PersonConverter()
    val emptyRDD = spark.sparkContext.emptyRDD[Person]

    val result = converter.convert(emptyRDD)

    result.count() shouldEqual 0
    result.schema shouldEqual converter.getGraphArSchema()
  }

  private def createTestPersonRDD(count: Int): RDD[Person] = {
    val persons = (1 to count).map { i =>
      val person = new Person()
      person.setAccountId(1000L + i)
      person.setFirstName(s"FirstName$i")
      person.setLastName(s"LastName$i")
      person.setGender(if (i % 2 == 0) 1.toByte else 0.toByte)
      person.setBirthday(System.currentTimeMillis() - i * 365L * 24 * 3600 * 1000)
      person.setCreationDate(new Date().getTime)
      person.setBrowserId(i)
      person.setCityId(i)
      person.setEmails(List(s"user$i@example.com").asJava)
      person.setLanguages(List(Integer.valueOf(1)).asJava)  // Language ID
      person
    }
    spark.sparkContext.parallelize(persons)
  }

  private def createTestPersonRDDWithIds(ids: Array[Long]): RDD[Person] = {
    val persons = ids.map { id =>
      val person = new Person()
      person.setAccountId(id)
      person.setFirstName(s"FirstName$id")
      person.setLastName(s"LastName$id")
      person.setGender(1.toByte)
      person.setBirthday(new Date().getTime)
      person.setCreationDate(new Date().getTime)
      person.setBrowserId(1)
      person.setCityId(1)
      person.setEmails(List(s"user$id@example.com").asJava)
      person.setLanguages(List(Integer.valueOf(1)).asJava)  // Language ID
      person
    }
    spark.sparkContext.parallelize(persons)
  }

  private def createTestPersonRDDWithNulls(): RDD[Person] = {
    val persons = Array(
      createPersonWithEmail("user1@example.com"),
      createPersonWithEmail(null),
      createPersonWithEmail("user3@example.com")
    )
    spark.sparkContext.parallelize(persons)
  }

  private def createPersonWithEmail(email: String): Person = {
    val person = new Person()
    person.setAccountId(Random.nextLong())
    person.setFirstName("TestFirstName")
    person.setLastName("TestLastName")
    person.setGender(1.toByte)
    person.setBirthday(new Date().getTime)
    person.setCreationDate(new Date().getTime)
    person.setBrowserId(1)
    person.setCityId(1)
    if (email != null) {
      person.setEmails(List(email).asJava)
    }
    person.setLanguages(List(Integer.valueOf(1)).asJava)  // Language ID
    person
  }
}