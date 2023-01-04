/** Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar.utils

import com.alibaba.graphar.GeneralParams

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

import scala.collection.SortedMap
import scala.collection.mutable.ArrayBuffer

/** IndexGenerator is an object to help generating the indices for vertex/edge DataFrames. */
object IndexGenerator {

  // index helper for the vertex DataFrame

  /** Generate a vertex index mapping from the primary key, the result DataFrame contains two columns: vertex index & primary key
   *
   * @param vertexDf  input vertex DataFrame.
   * @param primaryKey the primary key of vertex
   * @return a DataFrame contains two columns: vertex index & primary key.
   */
  def constructVertexIndexMapping(vertexDf: DataFrame, primaryKey: String): DataFrame = {
    val spark = vertexDf.sparkSession
    val schema = vertexDf.schema
    val id_index = schema.fieldIndex(primaryKey)
    val mapping_schema = StructType(Seq(StructField(GeneralParams.vertexIndexCol, LongType, false), schema.apply(id_index)))
    val rdd = vertexDf.rdd
    val counts = rdd
      .mapPartitionsWithIndex((i, ps) => Array((i, ps.size)).iterator, preservesPartitioning = true)
      .collectAsMap()
    val aggregatedCounts = SortedMap(counts.toSeq: _*)
      .foldLeft((0L, Map.empty[Int, Long])) { case ((total, map), (i, c)) =>
        (total + c, map + (i -> total))
      }
      ._2
    val broadcastedCounts = spark.sparkContext.broadcast(aggregatedCounts)
    val mapping = rdd.mapPartitionsWithIndex((i, ps) => {
      val start = broadcastedCounts.value(i)
      for { (p, j) <- ps.zipWithIndex } yield Row(start + j, p(id_index))
    })
    spark.createDataFrame(mapping, mapping_schema).withColumnRenamed(primaryKey, GeneralParams.primaryCol)
  }

  /** Add a column contains vertex index to DataFrame
   *
   * @param vertexDf the input vertex DataFrame.
   * @return DataFrame that contains a new vertex index column.
   */
  def generateVertexIndexColumn(vertexDf: DataFrame): DataFrame = {
    val spark = vertexDf.sparkSession
    val schema = vertexDf.schema
    val schema_with_index =  StructType(StructType(Seq(StructField(GeneralParams.vertexIndexCol, LongType, true)))++schema)
    val rdd = vertexDf.rdd
    val counts = rdd
      .mapPartitionsWithIndex((i, ps) => Array((i, ps.size)).iterator, preservesPartitioning = true)
      .collectAsMap()
    val aggregatedCounts = SortedMap(counts.toSeq: _*)
      .foldLeft((0L, Map.empty[Int, Long])) { case ((total, map), (i, c)) =>
        (total + c, map + (i -> total))
      }
      ._2
    val broadcastedCounts = spark.sparkContext.broadcast(aggregatedCounts)
    val rdd_with_index = rdd.mapPartitionsWithIndex((i, ps) => {
      val start = broadcastedCounts.value(i)
      for { (p, j) <- ps.zipWithIndex } yield Row.fromSeq(Seq(start + j) ++ p.toSeq)
    })
    spark.createDataFrame(rdd_with_index, schema_with_index)
  }

  // index helper for the Edge DataFrame

  /** Add a column contains edge index to input edge DataFrame. */
  def generateEdgeIndexColumn(edgeDf: DataFrame): DataFrame = {
    val spark = edgeDf.sparkSession
    val schema = edgeDf.schema
    val schema_with_index =  StructType(StructType(Seq(StructField(GeneralParams.edgeIndexCol, LongType, true)))++schema)
    val rdd = edgeDf.rdd
    val counts = rdd
      .mapPartitionsWithIndex((i, ps) => Array((i, ps.size)).iterator, preservesPartitioning = true)
      .collectAsMap()
    val aggregatedCounts = SortedMap(counts.toSeq: _*)
      .foldLeft((0L, Map.empty[Int, Long])) { case ((total, map), (i, c)) =>
        (total + c, map + (i -> total))
      }
      ._2
    val broadcastedCounts = spark.sparkContext.broadcast(aggregatedCounts)
    val rdd_with_index = rdd.mapPartitionsWithIndex((i, ps) => {
      val start = broadcastedCounts.value(i)
      for { (p, j) <- ps.zipWithIndex } yield Row.fromSeq(Seq(start + j) ++ p.toSeq)
    })
    spark.createDataFrame(rdd_with_index, schema_with_index)
  }

  /** Join the edge table with the vertex index mapping for source column. */
  def generateSrcIndexForEdgesFromMapping(edgeDf: DataFrame, srcColumnName: String, srcIndexMapping: DataFrame): DataFrame = {
    val spark = edgeDf.sparkSession
    srcIndexMapping.createOrReplaceTempView("src_vertex")
    edgeDf.createOrReplaceTempView("edge")
    val srcCol = GeneralParams.srcIndexCol;
    val indexCol = GeneralParams.vertexIndexCol;
    val srcPrimaryKey = GeneralParams.primaryCol;
    val trans_df = spark.sql(f"select src_vertex.$indexCol%s as $srcCol%s, edge.* from edge inner join src_vertex on src_vertex.$srcPrimaryKey%s=edge.$srcColumnName%s")
    // drop the old src id col
    trans_df.drop(srcColumnName)
	}

  /** Join the edge table with the vertex index mapping for destination column. */
  def generateDstIndexForEdgesFromMapping(edgeDf: DataFrame, dstColumnName: String, dstIndexMapping: DataFrame): DataFrame = {
    val spark = edgeDf.sparkSession
    dstIndexMapping.createOrReplaceTempView("dst_vertex")
    edgeDf.createOrReplaceTempView("edges")
    val dstCol = GeneralParams.dstIndexCol;
    val indexCol = GeneralParams.vertexIndexCol;
    val dstPrimaryKey = GeneralParams.primaryCol;
    val trans_df = spark.sql(f"select dst_vertex.$indexCol%s as $dstCol%s, edges.* from edges inner join dst_vertex on dst_vertex.$dstPrimaryKey%s=edges.$dstColumnName%s")
    // drop the old dst id col
    trans_df.drop(dstColumnName)
	}

  /** Join the edge table with the vertex index mapping for source & destination columns. */
  def generateSrcAndDstIndexForEdgesFromMapping(edgeDf: DataFrame, srcColumnName: String, dstColumnName: String, srcIndexMapping: DataFrame, dstIndexMapping: DataFrame): DataFrame = {
    val df_with_src_index = generateSrcIndexForEdgesFromMapping(edgeDf, srcColumnName, srcIndexMapping)
    generateDstIndexForEdgesFromMapping(df_with_src_index, dstColumnName, dstIndexMapping)
	}

  /** Construct vertex index for source column. */
  def generateSrcIndexForEdges(edgeDf: DataFrame, srcColumnName: String): DataFrame = {
    val srcDf = edgeDf.select(srcColumnName).distinct()
    val srcIndexMapping = constructVertexIndexMapping(srcDf, srcColumnName)
    generateSrcIndexForEdgesFromMapping(edgeDf, srcColumnName, srcIndexMapping)
	}

  /** Construct vertex index for destination column. */
  def generateDstIndexForEdges(edgeDf: DataFrame, dstColumnName: String): DataFrame = {
    val dstDf = edgeDf.select(dstColumnName).distinct()
    val dstIndexMapping = constructVertexIndexMapping(dstDf, dstColumnName)
    generateDstIndexForEdgesFromMapping(edgeDf, dstColumnName, dstIndexMapping)
	}

  /** Union and construct vertex index for source & destination columns. */
  def generateSrcAndDstIndexUnitedlyForEdges(edgeDf: DataFrame, srcColumnName: String, dstColumnName: String): DataFrame = {
    val srcDf = edgeDf.select(srcColumnName)
    val dstDf = edgeDf.select(dstColumnName)
    val primaryKey = GeneralParams.primaryCol;
    val vertexDf = srcDf.withColumnRenamed(srcColumnName, primaryKey).union(dstDf.withColumnRenamed(dstColumnName, primaryKey)).distinct()
    val vertexIndexMapping = constructVertexIndexMapping(vertexDf, primaryKey)
    generateSrcAndDstIndexForEdgesFromMapping(edgeDf, srcColumnName, dstColumnName, vertexIndexMapping, vertexIndexMapping)
	}
}
