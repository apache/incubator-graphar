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

package org.apache.graphar.datasources.ldbc.bridge

import ldbc.snb.datagen.entities.dynamic.person.Person
import ldbc.snb.datagen.entities.dynamic.relations.Knows
import org.apache.graphar.datasources.ldbc.model.ConversionResult
import org.apache.graphar.datasources.ldbc.converter.SparkTestBase
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.Date
import scala.collection.JavaConverters._
import scala.util.Success

class LdbcDirectBridgeTest extends AnyFunSuite with Matchers with SparkTestBase {

  test("LdbcDirectBridge should convert Person RDD to GraphAr format successfully") {
    implicit val sparkSession: SparkSession = spark
    val bridge = new LdbcDirectBridge()
    val personRDD = createTestPersonRDD(10)
    val outputPath = createTempDir()

    val result = bridge.convertToGraphAr(
      ldbcPersonRDD = personRDD,
      outputPath = outputPath,
      graphName = "test_graph",
      vertexChunkSize = 100L,
      edgeChunkSize = 100L,
      fileType = "csv"
    )

    result shouldBe a[Success[_]]

    val conversionResult = result.get
    conversionResult.personCount shouldEqual 10
    conversionResult.outputPath shouldEqual outputPath
  }

  test("LdbcDirectBridge should extract knows relationships correctly") {
    implicit val sparkSession: SparkSession = spark
    val bridge = new LdbcDirectBridge()
    val personRDD = createTestPersonRDDWithKnows()
    val outputPath = createTempDir()

    val result = bridge.convertToGraphAr(
      ldbcPersonRDD = personRDD,
      outputPath = outputPath,
      graphName = "knows_test_graph"
    )

    result shouldBe a[Success[_]]

    val conversionResult = result.get
    conversionResult.personCount shouldEqual 3
    conversionResult.knowsCount should be > 0L
  }

  test("LdbcDirectBridge should handle persons with various relationships") {
    implicit val sparkSession: SparkSession = spark
    val bridge = new LdbcDirectBridge()
    val personRDD = createTestPersonRDDWithRelationships()
    val outputPath = createTempDir()

    val result = bridge.convertToGraphAr(
      ldbcPersonRDD = personRDD,
      outputPath = outputPath,
      graphName = "relationships_test_graph"
    )

    result shouldBe a[Success[_]]

    val conversionResult = result.get
    conversionResult.personCount shouldEqual 5
    conversionResult.interestCount should be >= 0L
    conversionResult.workAtCount should be >= 0L
    conversionResult.studyAtCount should be >= 0L
    conversionResult.locationCount should be >= 0L
  }

  test("LdbcDirectBridge should handle empty Person RDD") {
    implicit val sparkSession: SparkSession = spark
    val bridge = new LdbcDirectBridge()
    val emptyRDD = spark.sparkContext.emptyRDD[Person]
    val outputPath = createTempDir()

    val result = bridge.convertToGraphAr(
      ldbcPersonRDD = emptyRDD,
      outputPath = outputPath,
      graphName = "empty_test_graph"
    )

    result shouldBe a[Success[_]]

    val conversionResult = result.get
    conversionResult.personCount shouldEqual 0
    conversionResult.knowsCount shouldEqual 0
  }

  test("LdbcDirectBridge should create proper ID mapping") {
    implicit val sparkSession: SparkSession = spark
    val bridge = new LdbcDirectBridge()
    val originalIds = Array(2001L, 2002L, 2003L)
    val personRDD = createTestPersonRDDWithSpecificIds(originalIds)
    val outputPath = createTempDir()

    val result = bridge.convertToGraphAr(
      ldbcPersonRDD = personRDD,
      outputPath = outputPath,
      graphName = "id_mapping_test_graph"
    )

    result shouldBe a[Success[_]]

    val conversionResult = result.get
    conversionResult.personCount shouldEqual 3
  }

  private def createTestPersonRDD(count: Int): RDD[Person] = {
    val persons = (1 to count).map { i =>
      val person = new Person()
      person.setAccountId(1000L + i)
      person.setFirstName(s"FirstName$i")
      person.setLastName(s"LastName$i")
      person.setGender(if (i % 2 == 0) 1.toByte else 0.toByte)
      person.setBirthday(new Date().getTime)
      person.setCreationDate(new Date().getTime)
      person.setBrowserId(i)
      person.setCityId(i)
      person.setEmails(List(s"user$i@example.com").asJava)
      person.setLanguages(List(Integer.valueOf(1), Integer.valueOf(2)).asJava)  // Language IDs
      person
    }
    spark.sparkContext.parallelize(persons)
  }

  private def createTestPersonRDDWithKnows(): RDD[Person] = {
    val person1 = createTestPerson(1001L, "Person1")
    val person2 = createTestPerson(1002L, "Person2")
    val person3 = createTestPerson(1003L, "Person3")

    val knows1 = new Knows(person2, new Date().getTime, new Date().getTime, 1.0f, false)
    val knows2 = new Knows(person3, new Date().getTime, new Date().getTime, 1.0f, false)
    person1.setKnows(List(knows1, knows2).asJava)

    val knows3 = new Knows(person1, new Date().getTime, new Date().getTime, 1.0f, false)
    person2.setKnows(List(knows3).asJava)

    spark.sparkContext.parallelize(Seq(person1, person2, person3))
  }

  private def createTestPersonRDDWithRelationships(): RDD[Person] = {
    val persons = (1 to 5).map { i =>
      val person = createTestPerson(1000L + i, s"Person$i")

      // Add interests
      person.setInterests(List(Integer.valueOf(i), Integer.valueOf(i+1)).asJava)  // Use Integer IDs

      // Add work information
      if (i % 2 == 0) {
        val companies = new java.util.HashMap[java.lang.Long, java.lang.Long]()
        companies.put(100L + i, new Date().getTime)
        person.setCompanies(companies)
      }

      // Add university information
      if (i % 3 == 0) {
        person.setUniversityLocationId(200 + i)
        person.setClassYear(2010 + i)
      }

      // Set city
      person.setCityId(300 + i)

      person
    }
    spark.sparkContext.parallelize(persons)
  }

  private def createTestPersonRDDWithSpecificIds(ids: Array[Long]): RDD[Person] = {
    val persons = ids.map { id =>
      createTestPerson(id, s"Person$id")
    }
    spark.sparkContext.parallelize(persons)
  }

  private def createTestPerson(id: Long, name: String): Person = {
    val person = new Person()
    person.setAccountId(id)
    person.setFirstName(name)
    person.setLastName("LastName")
    person.setGender(1.toByte)
    person.setBirthday(new Date().getTime)
    person.setCreationDate(new Date().getTime)
    person.setBrowserId(1)
    person.setCityId(1)
    person.setEmails(List(s"${name.toLowerCase}@example.com").asJava)
    person.setLanguages(List(Integer.valueOf(1)).asJava)  // Language ID
    person
  }

  private def createTempDir(): String = {
    val tempDir = File.createTempFile("ldbc_test", "")
    tempDir.delete()
    tempDir.mkdirs()
    tempDir.getAbsolutePath
  }
}