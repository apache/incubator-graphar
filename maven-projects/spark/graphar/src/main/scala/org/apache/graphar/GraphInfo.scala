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

package org.apache.graphar

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SparkSession}
import org.yaml.snakeyaml.{Yaml, DumperOptions}
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty
import org.yaml.snakeyaml.LoaderOptions

/** Main data type in gar enumeration */
object GarType extends Enumeration {
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

  /** List of same type */
  val LIST = Value(7)

  /**
   * Data type in gar to string.
   *
   * @param gar_type
   *   gar data type
   * @return
   *   string name of gar data type.
   */
  def GarTypeToString(gar_type: GarType.Value): String = gar_type match {
    case GarType.BOOL   => "bool"
    case GarType.INT32  => "int32"
    case GarType.INT64  => "int64"
    case GarType.FLOAT  => "float"
    case GarType.DOUBLE => "double"
    case GarType.STRING => "string"
    case GarType.LIST   => "list"
    case _ => throw new IllegalArgumentException("Unknown data type")
  }

  /**
   * String name to data type in gar.
   *
   * @param str
   *   the string name of gar data type
   * @return
   *   gar data type.
   */
  def StringToGarType(str: String): GarType.Value = str match {
    case "bool"   => GarType.BOOL
    case "int32"  => GarType.INT32
    case "int64"  => GarType.INT64
    case "float"  => GarType.FLOAT
    case "double" => GarType.DOUBLE
    case "string" => GarType.STRING
    case "list"   => GarType.LIST
    case _ => throw new IllegalArgumentException("Unknown data type: " + str)
  }
}

/** Type of  file format. */
object FileType extends Enumeration {
  type FileType = Value
  val CSV = Value(0)
  val PARQUET = Value(1)
  val ORC = Value(2)
  val JSON = Value(3)

  /**
   * File type to string.
   *
   * @param file_type
   *   file type in gar
   * @return
   *   string name of file type.
   */
  def FileTypeToString(file_type: FileType.Value): String = file_type match {
    case FileType.CSV     => "csv"
    case FileType.PARQUET => "parquet"
    case FileType.ORC     => "orc"
    case FileType.JSON    => "json"
    case _ => throw new IllegalArgumentException("Unknown file type")
  }

  /**
   * String to file type.
   *
   * @param str
   *   the string name of file type
   * @return
   *   file type in gar.
   */
  def StringToFileType(str: String): FileType.Value = str match {
    case "csv"     => FileType.CSV
    case "parquet" => FileType.PARQUET
    case "orc"     => FileType.ORC
    case "json"    => FileType.JSON
    case _ => throw new IllegalArgumentException("Unknown file type: " + str)
  }

}

/** Adj list type enumeration for adjacency list of graph. */
object AdjListType extends Enumeration {
  type AdjListType = Value

  /** collection of edges by source, but unordered, can represent COO format */
  val unordered_by_source = Value(0)

  /**
   * collection of edges by destination, but unordered, can represent COO format
   */
  val unordered_by_dest = Value(1)

  /**
   * collection of edges by source, ordered by source, can represent CSR format
   */
  val ordered_by_source = Value(2)

  /**
   * collection of edges by destination, ordered by destination, can represent
   * CSC format
   */
  val ordered_by_dest = Value(3)

  /** adjList type in gar to string */
  def AdjListTypeToString(adjList_type: AdjListType.Value): String =
    adjList_type match {
      case AdjListType.unordered_by_source => "unordered_by_source"
      case AdjListType.unordered_by_dest   => "unordered_by_dest"
      case AdjListType.ordered_by_source   => "ordered_by_source"
      case AdjListType.ordered_by_dest     => "ordered_by_dest"
      case _ => throw new IllegalArgumentException("Unknown adjList type")
    }

  /** String to adjList type in gar */
  def StringToAdjListType(str: String): AdjListType.Value = str match {
    case "unordered_by_source" => AdjListType.unordered_by_source
    case "unordered_by_dest"   => AdjListType.unordered_by_dest
    case "ordered_by_source"   => AdjListType.ordered_by_source
    case "ordered_by_dest"     => AdjListType.ordered_by_dest
    case _ => throw new IllegalArgumentException("Unknown adjList type: " + str)
  }
}

/** The property information of vertex or edge. */
class Property() {
  @BeanProperty var name: String = ""
  @BeanProperty var data_type: String = ""
  @BeanProperty var is_primary: Boolean = false
  private var is_nullable: Option[Boolean] = Option.empty

  /** Get data type in gar of the property */
  def getData_type_in_gar: GarType.Value = {
    GarType.StringToGarType(data_type)
  }

  override def equals(that: Any): Boolean = {
    that match {
      case other: Property =>
        this.name == other.name &&
          this.data_type == other.data_type &&
          this.is_primary == other.is_primary &&
          this.getIs_nullable == other.getIs_nullable
      case _ => false
    }
  }

  def toMap(): java.util.HashMap[String, Object] = {
    val data = new java.util.HashMap[String, Object]()
    data.put("name", name)
    data.put("data_type", data_type)
    data.put("is_primary", new java.lang.Boolean(is_primary))
    data.put("is_nullable", new java.lang.Boolean(getIs_nullable))
    return data
  }

  def getIs_nullable(): Boolean = {
    if (is_nullable.isEmpty) {
      !is_primary
    } else {
      is_nullable.get
    }
  }

  def setIs_nullable(is_nullable: Boolean): Unit = {
    this.is_nullable = Option(is_nullable)
  }
}

/** PropertyGroup is a class to store the property group information. */
class PropertyGroup() {
  @BeanProperty var prefix: String = ""
  @BeanProperty var file_type: String = ""
  @BeanProperty var properties = new java.util.ArrayList[Property]()

  override def equals(that: Any): Boolean = {
    that match {
      case other: PropertyGroup =>
        this.prefix == other.prefix &&
          this.file_type == other.file_type &&
          this.properties == other.properties
      case _ => false
    }
  }

  /** Get file type in gar of property group */
  def getFile_type_in_gar: FileType.Value = {
    FileType.StringToFileType(file_type)
  }

  def toMap(): java.util.HashMap[String, Object] = {
    val data = new java.util.HashMap[String, Object]()
    if (prefix != "") data.put("prefix", prefix)
    data.put("file_type", file_type)
    val property_num = properties.size()
    if (property_num > 0) {
      val property_maps = new java.util.ArrayList[Object]()
      for (i <- 0 until property_num) {
        property_maps.add(properties.get(i).toMap())
      }
      data.put("properties", property_maps)
    }
    return data
  }
}

/** AdjList is a class to store the adj list information of edge. */
class AdjList() {
  @BeanProperty var ordered: Boolean = false
  @BeanProperty var aligned_by: String = "src"
  @BeanProperty var prefix: String = ""
  @BeanProperty var file_type: String = ""

  override def equals(that: Any): Boolean = {
    that match {
      case other: AdjList =>
        this.ordered == other.ordered &&
          this.aligned_by == other.aligned_by &&
          this.prefix == other.prefix &&
          this.file_type == other.file_type
      case _ => false
    }
  }

  /** Get file type in gar of adj list */
  def getFile_type_in_gar: FileType.Value = {
    FileType.StringToFileType(file_type)
  }

  /** Get adj list type in string */
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

  def toMap(): java.util.HashMap[String, Object] = {
    val data = new java.util.HashMap[String, Object]()
    if (prefix != "") data.put("prefix", prefix)
    data.put("file_type", file_type)
    data.put("ordered", new java.lang.Boolean(ordered))
    data.put("aligned_by", aligned_by)
    return data
  }
}

/** GraphInfo is a class to store the graph meta information. */
class GraphInfo() {
  @BeanProperty var name: String = ""
  @BeanProperty var prefix: String = ""
  @BeanProperty var vertices = new java.util.ArrayList[String]()
  @BeanProperty var edges = new java.util.ArrayList[String]()
  @BeanProperty var version: String = ""

  var vertexInfos: Map[String, VertexInfo] = Map[String, VertexInfo]()
  var edgeInfos: Map[String, EdgeInfo] = Map[String, EdgeInfo]()

  def addVertexInfo(vertexInfo: VertexInfo): Unit = {
    vertexInfos += (vertexInfo.getLabel -> vertexInfo)
  }

  def addEdgeInfo(edgeInfo: EdgeInfo): Unit = {
    edgeInfos += (edgeInfo.getConcatKey() -> edgeInfo)
  }

  def getVertexInfo(label: String): VertexInfo = {
    vertexInfos(label)
  }

  def getEdgeInfo(
      srcLabel: String,
      edgeLabel: String,
      dstLabel: String
  ): EdgeInfo = {
    val key =
      srcLabel + GeneralParams.regularSeparator + edgeLabel + GeneralParams.regularSeparator + dstLabel
    edgeInfos(key)
  }

  def getVertexInfos(): Map[String, VertexInfo] = {
    return vertexInfos
  }

  def getEdgeInfos(): Map[String, EdgeInfo] = {
    return edgeInfos
  }

  /** Dump to Yaml string. */
  def dump(): String = {
    val data = new java.util.HashMap[String, Object]()
    data.put("name", name)
    data.put("vertices", vertices)
    data.put("edges", edges)
    data.put("prefix", prefix)
    data.put("version", version)
    val options = new DumperOptions()
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    options.setIndent(4)
    options.setPrettyFlow(true)
    options.setIndicatorIndent(2);
    val yaml = new Yaml(options)
    return yaml.dump(data)
  }
}

/** Helper object to load graph info files */
object GraphInfo {

  /** Load a yaml file from path and construct a GraphInfo from it. */
  def loadGraphInfo(graphInfoPath: String, spark: SparkSession): GraphInfo = {
    val path = new Path(graphInfoPath)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val input = fs.open(path)
    val yaml = new Yaml(
      new Constructor(classOf[GraphInfo], new LoaderOptions())
    )
    val graph_info = yaml.load(input).asInstanceOf[GraphInfo]
    if (graph_info.getPrefix == "") {
      val pos = graphInfoPath.lastIndexOf('/')
      if (pos != -1) {
        val prefix =
          graphInfoPath.substring(0, pos + 1) // +1 to include the slash
        graph_info.setPrefix(prefix)
      }
    }
    val prefix = graph_info.getPrefix
    val vertices_yaml = graph_info.getVertices
    val vertices_it = vertices_yaml.iterator
    while (vertices_it.hasNext()) {
      val file_name = vertices_it.next()
      val path = prefix + file_name
      val vertex_info = VertexInfo.loadVertexInfo(path, spark)
      graph_info.addVertexInfo(vertex_info)
    }
    val edges_yaml = graph_info.getEdges
    val edges_it = edges_yaml.iterator
    while (edges_it.hasNext()) {
      val file_name = edges_it.next()
      val path = prefix + file_name
      val edge_info = EdgeInfo.loadEdgeInfo(path, spark)
      graph_info.addEdgeInfo(edge_info)
    }
    return graph_info
  }
}
