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
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty

object GarType extends Enumeration{
  type GarType = Value
  val BOOL = Value(0)
  val INT32 = Value(2)
  val INT64 = Value(3)
  val FLOAT = Value(4)
  val DOUBLE = Value(5)
  val STRING = Value(6)

  def GarTypeToString(gar_type: GarType.Value): String = gar_type match {
    case GarType.BOOL => "bool"
    case GarType.INT32 => "int32"
    case GarType.INT64 => "int64"
    case GarType.FLOAT => "float"
    case GarType.DOUBLE => "double"
    case GarType.STRING => "string"
    case _ => throw new IllegalArgumentException
  }

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

object FileType extends Enumeration {
  type FileType = Value
  val CSV = Value(0)
  val PARQUET = Value(1)
  val ORC = Value(2)

  def FileTypeToString(file_type: FileType.Value): String = file_type match {
    case FileType.CSV => "csv"
    case FileType.PARQUET => "parquet"
    case FileType.ORC => "orc"
    case _ => throw new IllegalArgumentException
  }

  def StringToFileType(str: String): FileType.Value = str match {
    case "csv" => FileType.CSV
    case "parquet" => FileType.PARQUET
    case "orc" => FileType.ORC
    case _ => throw new IllegalArgumentException
  }

}

object AdjListType extends Enumeration {
  type AdjListType = Value
  val unordered_by_source = Value(0)
  val unordered_by_dest = Value(1)
  val ordered_by_source = Value(2)
  val ordered_by_dest = Value(3)

  def AdjListTypeToString(adjList_type: AdjListType.Value): String = adjList_type match {
    case AdjListType.unordered_by_source => "unordered_by_source"
    case AdjListType.unordered_by_dest => "unordered_by_dest"
    case AdjListType.ordered_by_source => "ordered_by_source"
    case AdjListType.ordered_by_dest => "ordered_by_dest"
    case _ => throw new IllegalArgumentException
  }

  def StringToAdjListType(str: String): AdjListType.Value = str match {
    case "unordered_by_source" => AdjListType.unordered_by_source
    case "unordered_by_dest" => AdjListType.unordered_by_dest
    case "ordered_by_source" => AdjListType.ordered_by_source
    case "ordered_by_dest" => AdjListType.ordered_by_dest
    case _ => throw new IllegalArgumentException
  }
}

class Property () {
  @BeanProperty var name: String = ""
  @BeanProperty var data_type: String = ""
  @BeanProperty var is_primary: Boolean = false

  def getData_type_in_gar: GarType.Value = {
    GarType.StringToGarType(data_type)
  }
}

class PropertyGroup () {
  @BeanProperty var prefix: String = ""
  @BeanProperty var file_type: String = ""
  @BeanProperty var properties = new java.util.ArrayList[Property]()

  def getFile_type_in_gar: FileType.Value = {
    FileType.StringToFileType(file_type)
  }
}

class AdjList () {
  @BeanProperty var ordered: Boolean = false
  @BeanProperty var aligned_by: String = "src"
  @BeanProperty var prefix: String = ""
  @BeanProperty var file_type: String = ""
  @BeanProperty var property_groups = new java.util.ArrayList[PropertyGroup]()

  def getFile_type_in_gar: FileType.Value = {
    FileType.StringToFileType(file_type)
  }

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

  def getAdjList_type_in_gar: AdjListType.Value = {
    AdjListType.StringToAdjListType(getAdjList_type)
  }
}

class GraphInfo() {
  @BeanProperty var name: String = ""
  @BeanProperty var prefix: String = ""
  @BeanProperty var vertices = new java.util.ArrayList[String]()
  @BeanProperty var edges = new java.util.ArrayList[String]()
  @BeanProperty var version: String = ""
}
