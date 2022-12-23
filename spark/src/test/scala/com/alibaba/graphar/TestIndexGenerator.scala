package com.alibaba.graphar

import com.alibaba.graphar.utils.IndexGenerator

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class IndexGeneratorSuite extends AnyFunSuite {
  val spark = SparkSession.builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("generate vertex index") {
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_0_0.csv").getPath
    val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)
    assertThrows[IllegalArgumentException](vertex_df.schema.fieldIndex(GeneralParams.vertexIndexCol))
    val df_with_index = IndexGenerator.generateVertexIndexColumn(vertex_df)
    val field_index = df_with_index.schema(GeneralParams.vertexIndexCol)
    val desc = df_with_index.describe(GeneralParams.vertexIndexCol)
  }

  test("generate edge index") {
    val file_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv").getPath
    val edge_df = spark.read.option("delimiter", "|").option("header", "true").csv(file_path)
    val df_with_index = IndexGenerator.generateSrcAndDstIndexUnitedlyForEdges(edge_df, "src", "dst")
    df_with_index.show()
  }

  test("generate edge index with vertex") {
    val vertex_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_0_0.csv").getPath
    val edge_path = getClass.getClassLoader.getResource("gar-test/ldbc_sample/person_knows_person_0_0.csv").getPath
    val vertex_df = spark.read.option("delimiter", "|").option("header", "true").csv(vertex_path)
    val edge_df = spark.read.option("delimiter", "|").option("header", "true").csv(edge_path)
    val vertex_mapping = IndexGenerator.constructVertexIndexMapping(vertex_df, "id")
    val edge_df_src_index = IndexGenerator.generateSrcIndexForEdgesFromMapping(edge_df, "src", vertex_mapping)
    edge_df_src_index.show()
    val edge_df_src_dst_index = IndexGenerator.generateDstIndexForEdgesFromMapping(edge_df_src_index, "dst", vertex_mapping)
    edge_df_src_dst_index.show()
  }

  }
