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
import org.apache.graphar.datasources.ldbc.model.{PersonHasInterestData, ValidationResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

/**
 * Person has interest Tag relationship converter
 * Extracts interest relationships from Person entities
 */
class PersonHasInterestConverter extends LdbcDataConverter[Person, PersonHasInterestData] {

  def convertWithIdMapping(
    personRDD: RDD[Person],
    idMapping: Map[Long, Long]
  )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val interestDataRDD = personRDD.flatMap { person =>
      Option(person.getInterests) match {
        case Some(interests) =>
          interests.asScala.map { tagId =>
            PersonHasInterestData(
              personId = idMapping.getOrElse(person.getAccountId, -1L),
              tagId = tagId,
              creationDate = person.getCreationDate
            )
          }
        case None => Seq.empty
      }
    }.filter(_.personId != -1L) // Filter out invalid mappings

    interestDataRDD.toDF().select(getGraphArSchema().fieldNames.map(col): _*)
  }

  override def convert(rdd: RDD[Person])(implicit spark: SparkSession): DataFrame = {
    // This method requires ID mapping, use convertWithIdMapping instead
    throw new UnsupportedOperationException("Use convertWithIdMapping method instead")
  }

  override def getGraphArSchema(): StructType = {
    StructType(Seq(
      StructField("personId", LongType, nullable = false),
      StructField("tagId", IntegerType, nullable = false),
      StructField("creationDate", LongType, nullable = false)
    ))
  }
}