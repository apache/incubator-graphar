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
import ldbc.snb.datagen.util.GeneratorConfiguration
import org.apache.graphar.datasources.ldbc.converter._
import org.apache.graphar.datasources.ldbc.generator.LdbcDriverGenerator
import org.apache.graphar.datasources.ldbc.model.{ConversionResult, KnowsData}
import org.apache.graphar.graph.GraphWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.Try
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

class LdbcDirectBridge(
                        personConverter: PersonConverter = new PersonConverter(),
                        interestConverter: PersonHasInterestConverter = new PersonHasInterestConverter(),
                        workAtConverter: PersonWorkAtConverter = new PersonWorkAtConverter(),
                        studyAtConverter: PersonStudyAtConverter = new PersonStudyAtConverter(),
                        locationConverter: PersonIsLocatedInConverter = new PersonIsLocatedInConverter()
                      ) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[LdbcDirectBridge])

  def convertToGraphArFromConfig(
                                  config: GeneratorConfiguration,
                                  outputPath: String,
                                  graphName: String = "ldbc_social_network",
                                  vertexChunkSize: Long = 1024L,
                                  edgeChunkSize: Long = 1024L,
                                  fileType: String = "csv"
                                )(implicit spark: SparkSession): Try[ConversionResult] = Try {

    logger.info("=== STARTING DRIVER-SIDE LDBC GENERATION ===")
    val driverGenerator = new LdbcDriverGenerator(config)
    val allPersons = driverGenerator.generateAllPersons()
    logger.info(s"✓ Generated ${allPersons.size} persons on Driver side")

    val personRDD = spark.sparkContext.parallelize(allPersons)
    convertToGraphArInternal(personRDD, outputPath, graphName, vertexChunkSize, edgeChunkSize, fileType).get
  }

  def convertToGraphAr(
                        ldbcPersonRDD: RDD[Person],
                        outputPath: String,
                        graphName: String = "ldbc_direct_graph",
                        vertexChunkSize: Long = 1024L,
                        edgeChunkSize: Long = 1024L,
                        fileType: String = "csv"
                      )(implicit spark: SparkSession): Try[ConversionResult] =
    convertToGraphArInternal(ldbcPersonRDD, outputPath, graphName, vertexChunkSize, edgeChunkSize, fileType)

  private def convertToGraphArInternal(
                                        ldbcPersonRDD: RDD[Person],
                                        outputPath: String,
                                        graphName: String,
                                        vertexChunkSize: Long,
                                        edgeChunkSize: Long,
                                        fileType: String
                                      )(implicit spark: SparkSession): Try[ConversionResult] = Try {

    val personDF = personConverter.convert(ldbcPersonRDD).cache()
    val personCount = personDF.count()

    val idMapping = createIdMapping(personDF)

    val knowsDF = extractKnowsFromPersons(ldbcPersonRDD, idMapping)
    val interestDF = interestConverter.convertWithIdMapping(ldbcPersonRDD, idMapping)
    val workAtDF = workAtConverter.convertWithIdMapping(ldbcPersonRDD, idMapping)
    val studyAtDF = studyAtConverter.convertWithIdMapping(ldbcPersonRDD, idMapping)
    val locationDF = locationConverter.convertWithIdMapping(ldbcPersonRDD, idMapping)

    writeToGraphAr(
      personDF, knowsDF, interestDF, workAtDF, studyAtDF, locationDF,
      outputPath, graphName, vertexChunkSize, edgeChunkSize, fileType
    )

    personDF.unpersist()

    ConversionResult(
      personCount = personCount,
      knowsCount = knowsDF.count(),
      interestCount = interestDF.count(),
      workAtCount = workAtDF.count(),
      studyAtCount = studyAtDF.count(),
      locationCount = locationDF.count(),
      outputPath = outputPath
    )
  }

  private def createIdMapping(personDF: DataFrame): Map[Long, Long] = {
    personDF
      .withColumn("originalId", col("originalId").cast("long"))
      .withColumn("id", col("id").cast("long"))
      .select("originalId", "id")
      .rdd
      .map(row => row.getLong(0) -> row.getLong(1))
      .collectAsMap()
      .toMap
  }

  private def extractKnowsFromPersons(
                                       ldbcPersonRDD: RDD[Person],
                                       idMapping: Map[Long, Long]
                                     )(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    ldbcPersonRDD.flatMap { person =>
      Option(person.getKnows).map(_.asScala.map { knows =>
        KnowsData(
          src = idMapping.getOrElse(person.getAccountId, -1L),
          dst = idMapping.getOrElse(knows.to().getAccountId, -1L),
          creationDate = knows.getCreationDate,
          weight = knows.getWeight
        )
      }).getOrElse(Seq.empty)
    }.filter(k => k.src != -1L && k.dst != -1L).toDF()
  }

  private def writeToGraphAr(
                              personDF: DataFrame,
                              knowsDF: DataFrame,
                              interestDF: DataFrame,
                              workAtDF: DataFrame,
                              studyAtDF: DataFrame,
                              locationDF: DataFrame,
                              outputPath: String,
                              graphName: String,
                              vertexChunkSize: Long,
                              edgeChunkSize: Long,
                              fileType: String
                            )(implicit spark: SparkSession): Unit = {

    def castFloatToDouble(df: DataFrame): DataFrame = {
      df.select(df.schema.map { field =>
        if (field.dataType == FloatType) col(field.name).cast("double").as(field.name)
        else col(field.name)
      }: _*)
    }



    def emptyVertexDF(): DataFrame = {
      val schema = StructType(Seq(StructField("id", LongType, nullable = false)))
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    }

    val personDFFixed   = castFloatToDouble(personDF)
    val knowsDFFixed    = castFloatToDouble(knowsDF)
    val interestDFFixed = castFloatToDouble(interestDF)
    val workAtDFFixed   = castFloatToDouble(workAtDF)
    val studyAtDFFixed  = castFloatToDouble(studyAtDF)
    val locationDFFixed = castFloatToDouble(locationDF)

    val writer = new GraphWriter()

    // Register all necessary vertices
    // A GraphAr vertex must have an id column, even if it is an empty DataFrame; otherwise, the IndexGenerator will fail to access the column when generating indexes, and an ArrayIndexOutOfBoundsException will be thrown.
    writer.PutVertexData("Person", personDFFixed)
    if (studyAtDFFixed.count() > 0) writer.PutVertexData("University", emptyVertexDF())
    if (workAtDFFixed.count() > 0) writer.PutVertexData("Organisation", emptyVertexDF())
    if (locationDFFixed.count() > 0) writer.PutVertexData("Place", emptyVertexDF())
    if (interestDFFixed.count() > 0) writer.PutVertexData("Tag", emptyVertexDF())

    // 写 edge
    if (knowsDFFixed.count() > 0) writer.PutEdgeData(("Person", "knows", "Person"), knowsDFFixed)
    if (interestDFFixed.count() > 0) writer.PutEdgeData(("Person", "hasInterest", "Tag"), interestDFFixed)
    if (workAtDFFixed.count() > 0) writer.PutEdgeData(("Person", "workAt", "Organisation"), workAtDFFixed)
    if (studyAtDFFixed.count() > 0) writer.PutEdgeData(("Person", "studyAt", "University"), studyAtDFFixed)
    if (locationDFFixed.count() > 0) writer.PutEdgeData(("Person", "isLocatedIn", "Place"), locationDFFixed)

    writer.write(
      path = outputPath,
      spark = spark,
      name = graphName,
      vertex_chunk_size = vertexChunkSize,
      edge_chunk_size = edgeChunkSize,
      file_type = fileType
    )
  }
}
