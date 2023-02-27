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

import java.io.{File, FileInputStream}
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.{SparkSession}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty

/** Main data type in gar enumeration */
object GarType extends Enumeration{
  type GarType = Value
  /** Boolean type */
  val BOOL = Value(0)
  /** Signed 32-bit integer */
  val INT32 = Value(2)
  /** Signed 64-bit integer */
  val INT64 = Value(3)
  /** 4-byte floating point value */
  val FLOAT = Value(4)
  /** 8-byte floating point value */
  val DOUBLE = Value(5)
  /** UTF8 variable-length string */
  val STRING = Value(6)

  /** Data type in gar to string.
   *
   * @param gar_type gar data type
   * @return string name of gar data type.
   *
   */
  def GarTypeToString(gar_type: GarType.Value): String = gar_type match {
    case GarType.BOOL => "bool"
    case GarType.INT32 => "int32"
    case GarType.INT64 => "int64"
    case GarType.FLOAT => "float"
    case GarType.DOUBLE => "double"
    case GarType.STRING => "string"
    case _ => throw new IllegalArgumentException
  }

  /** String name to data type in gar.
   *
   * @param str the string name of gar data type
   * @return gar data type.
   *
   */
  def StringToGarType(str: String): GarType.Value = str match {
    case "bool" => GarType.BOOL
    case "int32" => GarType.INT32
    case "int64" => GarType.INT64
    case "float" => GarType.FLOAT
    case "double" => GarType.DOUBLE
    case "string" => GarType.STRING
    case _ => throw new IllegalArgumentException
  }
}

/** Type of  file format. */
object FileType extends Enumeration {
  type FileType = Value
  val CSV = Value(0)
  val PARQUET = Value(1)
  val ORC = Value(2)

  /** File type to string.
   *
   * @param file_type  file type in gar
   * @return string name of file type.
   *
   */
  def FileTypeToString(file_type: FileType.Value): String = file_type match {
    case FileType.CSV => "csv"
    case FileType.PARQUET => "parquet"
    case FileType.ORC => "orc"
    case _ => throw new IllegalArgumentException
  }

  /** String to file type.
   *
   * @param str the string name of file type
   * @return file type in gar.
   *
   */
  def StringToFileType(str: String): FileType.Value = str match {
    case "csv" => FileType.CSV
    case "parquet" => FileType.PARQUET
    case "orc" => FileType.ORC
    case _ => throw new IllegalArgumentException
  }

}

/** Adj list type enumeration for adjacency list of graph. */
object AdjListType extends Enumeration {
  type AdjListType = Value
  /** collection of edges by source, but unordered, can represent COO format */
  val unordered_by_source = Value(0)
  /** collection of edges by destination, but unordered, can represent COO format */
  val unordered_by_dest = Value(1)
  /** collection of edges by source, ordered by source, can represent CSR format */
  val ordered_by_source = Value(2)
  /** collection of edges by destination, ordered by destination, can represent CSC format */
  val ordered_by_dest = Value(3)

  /** adjList type in gar to string */
  def AdjListTypeToString(adjList_type: AdjListType.Value): String = adjList_type match {
    case AdjListType.unordered_by_source => "unordered_by_source"
    case AdjListType.unordered_by_dest => "unordered_by_dest"
    case AdjListType.ordered_by_source => "ordered_by_source"
    case AdjListType.ordered_by_dest => "ordered_by_dest"
    case _ => throw new IllegalArgumentException
  }

  /** String to adjList type in gar */
  def StringToAdjListType(str: String): AdjListType.Value = str match {
    case "unordered_by_source" => AdjListType.unordered_by_source
    case "unordered_by_dest" => AdjListType.unordered_by_dest
    case "ordered_by_source" => AdjListType.ordered_by_source
    case "ordered_by_dest" => AdjListType.ordered_by_dest
    case _ => throw new IllegalArgumentException
  }
}

/** The property information of vertex or edge. */
class Property () {
  @BeanProperty var name: String = ""
  @BeanProperty var data_type: String = ""
  @BeanProperty var is_primary: Boolean = false

  /** Get data type in gar of the property */
  def getData_type_in_gar: GarType.Value = {
    GarType.StringToGarType(data_type)
  }
}

/** PropertyGroup is a class to store the property group information. */
class PropertyGroup () {
  @BeanProperty var prefix: String = ""
  @BeanProperty var file_type: String = ""
  @BeanProperty var properties = new java.util.ArrayList[Property]()

  /** Get file type in gar of property group */
  def getFile_type_in_gar: FileType.Value = {
    FileType.StringToFileType(file_type)
  }
}

/** AdjList is a class to store the adj list information of edge. */
class AdjList () {
  @BeanProperty var ordered: Boolean = false
  @BeanProperty var aligned_by: String = "src"
  @BeanProperty var prefix: String = ""
  @BeanProperty var file_type: String = ""
  @BeanProperty var property_groups = new java.util.ArrayList[PropertyGroup]()

  /** Get file type in gar of adj list */
  def getFile_type_in_gar: FileType.Value = {
    FileType.StringToFileType(file_type)
  }

  /** Get adj list type in string*/
  def getAdjList_type: String = {
    var str: String = ""
    if (ordered) {
      str = "ordered_by_"
    } else {
      str = "unordered_by_"
    }
    if (aligned_by == "src") {
      str += "source"
    } else {
      str += "dest"
    }
    return str
  }

  /** Get adj list type in gar */
  def getAdjList_type_in_gar: AdjListType.Value = {
    AdjListType.StringToAdjListType(getAdjList_type)
  }
}

/** GraphInfo is a class to store the graph meta information. */
class GraphInfo() {
  @BeanProperty var name: String = ""
  @BeanProperty var prefix: String = ""
  @BeanProperty var vertices = new java.util.ArrayList[String]()
  @BeanProperty var edges = new java.util.ArrayList[String]()
  @BeanProperty var version: String = ""
}

/** Helper object to load graph info files */
object GraphInfo {
  /** Load a yaml file from path and construct a GraphInfo from it. */
  def loadGraphInfo(graphInfoPath: String, spark: SparkSession): GraphInfo = {
    val path = new Path(graphInfoPath)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val input = fs.open(path)
    val yaml = new Yaml(new Constructor(classOf[GraphInfo]))
    val graph_info = yaml.load(input).asInstanceOf[GraphInfo]
    if (graph_info.getPrefix == "") {
      val pos = graphInfoPath.lastIndexOf('/')
      if (pos != -1) {
        val prefix = graphInfoPath.substring(0, pos + 1) // +1 to include the slash
        graph_info.setPrefix(prefix)
      }
    }
    return graph_info
  }
}
