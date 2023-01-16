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

package com.alibaba.graphar

import com.alibaba.graphar.datasources._

import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class ReaderSuite extends AnyFunSuite {
  val spark = SparkSession.builder()
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  test("read files directly") {
    // read parquet files (vertex chunks)
    val parquet_file_path = "gar-test/ldbc_sample/parquet"
    val parquet_prefix = getClass.getClassLoader.getResource(parquet_file_path).getPath
    val parqeut_read_path = parquet_prefix + "/vertex/person/id"
    val df1 = spark.read.option("fileFormat", "parquet").format("com.alibaba.graphar.datasources.GarDataSource").load(parqeut_read_path)
    // validate reading results
    assert(df1.rdd.getNumPartitions == 10)
    assert(df1.count() == 903)
    println(df1.rdd.collect().mkString("\n"))

    // read orc files (vertex chunks)
    val orc_file_path = "gar-test/ldbc_sample/orc"
    val orc_prefix = getClass.getClassLoader.getResource(orc_file_path).getPath
    val orc_read_path = orc_prefix + "/vertex/person/id"
    val df2 = spark.read.option("fileFormat", "orc").format("com.alibaba.graphar.datasources.GarDataSource").load(orc_read_path)
    // compare
    assert(df2.rdd.collect().deep == df1.rdd.collect().deep)

    // read csv files recursively (edge chunks)
    val csv_file_path = "gar-test/ldbc_sample/csv"
    val csv_prefix = getClass.getClassLoader.getResource(csv_file_path).getPath
    val csv_read_path = csv_prefix + "/edge/person_knows_person/ordered_by_source/adj_list"
    val df3 = spark.read.option("fileFormat", "csv").option("recursiveFileLookup", "true").format("com.alibaba.graphar.datasources.GarDataSource").load(csv_read_path)
    // validate reading results
    assert(df3.rdd.getNumPartitions == 11)
    assert(df3.count() == 6626)
    println(df3.rdd.collect().mkString("\n"))

    // throw an exception for unsupported file formats
    assertThrows[IllegalArgumentException](spark.read.option("fileFormat", "invalid").format("com.alibaba.graphar.datasources.GarDataSource").load(csv_read_path))
  }
}
