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

import com.alibaba.graphar.datasources.garparquet._

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

  test("read reader v2") {
    val file_path = "gar-test/ldbc_sample/parquet"
    val prefix = getClass.getClassLoader.getResource(file_path).getPath
    val read_path = prefix + "/vertex/person/id"
    println(read_path)
    val df = spark.read.format("com.alibaba.graphar.datasources.garparquet.GarParquetDataSource").load(read_path)
    df.show()
    println(df.count())
    println(df.rdd.collect().mkString("\n"))
  }
}
