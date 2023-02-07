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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

/** Helper object to concat DataFrames */
object DataFrameConcat {
  /** Concat two DataFrames.
   *
   * @param df1 The first DataFrame.
   * @param df2 The second DataFrame.
   * @return The result DataFrame that concats the two DataFrames.
   */
  def concat(df1: DataFrame, df2: DataFrame): DataFrame = {
    val spark = df1.sparkSession
    val schema = StructType(Array.concat(df1.schema.fields, df2.schema.fields))
    val res_rdd = df1.rdd.zip(df2.rdd).map(pair => Row.fromSeq(pair._1.toSeq.toList ::: pair._2.toSeq.toList))
    val df = spark.createDataFrame(res_rdd, schema)
    return df
  }

  /** Concat two RDDs.
   *
   * @param rdd1 The first RDD.
   * @param rdd2 The second RDD.
   * @return The result RDD that concats the two RDDs.
   */
  def concatRdd(rdd1: RDD[Row], rdd2: RDD[Row]): RDD[Row] = {
    rdd1.zip(rdd2).map(pair => Row.fromSeq(pair._1.toSeq.toList ::: pair._2.toSeq.toList))
  }
}
