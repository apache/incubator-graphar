/*
 * Copyright 2022-2023 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.graphar

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.yaml.snakeyaml.{DumperOptions, Yaml}
import org.yaml.snakeyaml.constructor.Constructor

import scala.beans.BeanProperty
import org.yaml.snakeyaml.LoaderOptions

/** Edge info is a class to store the edge meta information. */
class EdgeInfo() {
  @BeanProperty var src_label: String = ""
  @BeanProperty var edge_label: String = ""
  @BeanProperty var dst_label: String = ""
  @BeanProperty var chunk_size: Long = 0
  @BeanProperty var src_chunk_size: Long = 0
  @BeanProperty var dst_chunk_size: Long = 0
  @BeanProperty var directed: Boolean = false
  @BeanProperty var prefix: String = ""
  @BeanProperty var adj_lists = new java.util.ArrayList[AdjList]()
  @BeanProperty var property_groups = new java.util.ArrayList[PropertyGroup]()
  @BeanProperty var version: String = ""

  /**
   * Check if the edge info supports the adj list type.
   *
   * @param adj_list_type
   *   adjList type in gar to check.
   * @return
   *   true if edge info supports the adj list type, otherwise return false.
   */
  def containAdjList(adj_list_type: AdjListType.Value): Boolean = {
    val tot: Int = adj_lists.size
    for (k <- 0 to tot - 1) {
      val adj_list = adj_lists.get(k)
      if (adj_list.getAdjList_type_in_gar == adj_list_type) {
        return true
      }
    }
    return false
  }

  /**
   * Get path prefix of adj list type.
   *
   * @param adj_list_type
   *   The input adj list type in gar.
   * @return
   *   path prefix of the adj list type, if edge info not support the adj list
   *   type, raise an IllegalArgumentException error.
   */
  def getAdjListPrefix(adj_list_type: AdjListType.Value): String = {
    val tot: Int = adj_lists.size
    for (k <- 0 to tot - 1) {
      val adj_list = adj_lists.get(k)
      if (adj_list.getAdjList_type_in_gar == adj_list_type) {
        var str: String = adj_list.getPrefix
        if (str == "") {
          str = AdjListType.AdjListTypeToString(adj_list_type) + "/"
        }
        return str
      }
    }
    throw new IllegalArgumentException(
      "adj list type not found: " + AdjListType.AdjListTypeToString(
        adj_list_type
      )
    )
  }

  /**
   * Get the adj list topology chunk file type of adj list type.
   *
   * @param adj_list_type
   *   the input adj list type.
   * @return
   *   file format type in gar of the adj list type, if edge info not support
   *   the adj list type, raise an IllegalArgumentException error.
   */
  def getAdjListFileType(adj_list_type: AdjListType.Value): FileType.Value = {
    val tot: Int = adj_lists.size
    for (k <- 0 to tot - 1) {
      val adj_list = adj_lists.get(k)
      if (adj_list.getAdjList_type_in_gar == adj_list_type) {
        return adj_list.getFile_type_in_gar
      }
    }
    throw new IllegalArgumentException(
      "adj list type not found: " + AdjListType.AdjListTypeToString(
        adj_list_type
      )
    )
  }

  /**
   * Check if the edge info contains the property group.
   *
   * @param property_group
   *   the property group to check.
   *
   * @return
   *   true if the edge info contains the property group in certain adj list
   *   structure. If edge info not support the given adj list type or not
   *   contains the property group in the adj list structure, return false.
   */
  def containPropertyGroup(
      property_group: PropertyGroup
  ): Boolean = {
    val len: Int = property_groups.size
    for (i <- 0 to len - 1) {
      val pg: PropertyGroup = property_groups.get(i)
      if (pg == property_group) {
        return true
      }
    }
    return false
  }

  /**
   * Check if the edge info contains the property.
   *
   * @param property_name
   *   name of the property.
   * @return
   *   true if edge info contains the property, otherwise false.
   */
  def containProperty(property_name: String): Boolean = {
    val len: Int = property_groups.size
    for (i <- 0 to len - 1) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      for (j <- 0 to num - 1) {
        if (properties.get(j).getName == property_name) {
          return true
        }
      }
    }
    return false
  }

  /**
   * Get the property group that contains property.
   *
   * @param property_name
   *   name of the property.
   * @return
   *   property group that contains the property, otherwise raise
   *   IllegalArgumentException error.
   */
  def getPropertyGroup(
      property_name: String
  ): PropertyGroup = {
    val len: Int = property_groups.size
    for (i <- 0 to len - 1) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      for (j <- 0 to num - 1) {
        if (properties.get(j).getName == property_name) {
          return pg
        }
      }
    }
    throw new IllegalArgumentException("Property not found: " + property_name)
  }

  /**
   * Get the data type of property.
   *
   * @param property_name
   *   name of the property.
   * @return
   *   data type in gar of the property. If edge info not contains the property,
   *   raise an IllegalArgumentException error.
   */
  def getPropertyType(property_name: String): GarType.Value = {
    val len: Int = property_groups.size
    for (i <- 0 to len - 1) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      for (j <- 0 to num - 1) {
        if (properties.get(j).getName == property_name) {
          return properties.get(j).getData_type_in_gar
        }
      }
    }
    throw new IllegalArgumentException("Property not found: " + property_name)
  }

  /**
   * Check the property is primary key of edge info.
   *
   * @param property_name
   *   name of the property.
   * @return
   *   true if the property is the primary key of edge info, false if not. If
   *   edge info not contains the property, raise an IllegalArgumentException
   *   error.
   */
  def isPrimaryKey(property_name: String): Boolean = {
    val len: Int = property_groups.size
    for (i <- 0 to len - 1) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      for (j <- 0 to num - 1) {
        if (properties.get(j).getName == property_name) {
          return properties.get(j).getIs_primary
        }
      }
    }
    throw new IllegalArgumentException("Property not found: " + property_name)
  }

  /**
   * Check the property is nullable key of edge info.
   *
   * @param property_name
   *   name of the property.
   * @return
   *   true if the property is the nullable key of edge info, false if not. If
   *   edge info not contains the property, raise an IllegalArgumentException
   *   error.
   */
  def isNullableKey(property_name: String): Boolean = {
    val len: Int = property_groups.size
    for (i <- 0 to len - 1) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      for (j <- 0 to num - 1) {
        if (properties.get(j).getName == property_name) {
          return properties.get(j).getIs_nullable()
        }
      }
    }
    throw new IllegalArgumentException("Property not found: " + property_name)
  }

  /** Get Primary key of edge info. */
  def getPrimaryKey(): String = {
    val len: Int = property_groups.size
    for (i <- 0 to len - 1) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      for (j <- 0 to num - 1) {
        if (properties.get(j).getIs_primary) {
          return properties.get(j).getName
        }
      }
    }
    return ""
  }

  /** Check if the edge info is validated. */
  def isValidated(): Boolean = {
    if (src_label == "" || edge_label == "" || dst_label == "") {
      return false
    }
    if (chunk_size <= 0 || src_chunk_size <= 0 || dst_chunk_size <= 0) {
      return false
    }
    val tot: Int = adj_lists.size
    for (k <- 0 to tot - 1) {
      val adj_list = adj_lists.get(k)
      val file_type = adj_list.getFile_type_in_gar
    }
    val len: Int = property_groups.size
    for (i <- 0 to len - 1) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      if (num == 0) {
        return false
      }
      val file_type = pg.getFile_type_in_gar
    }
    return true
  }

  /**
   * Get the vertex num file path
   *
   * @param adj_list_type
   *   type of adj list structure.
   * @return
   *   the vertex num file path. If edge info not support the adj list type,
   *   raise an IllegalArgumentException error.
   */
  def getVerticesNumFilePath(adj_list_type: AdjListType.Value): String = {
    if (containAdjList(adj_list_type) == false) {
      throw new IllegalArgumentException(
        "adj list type not found: " + AdjListType.AdjListTypeToString(
          adj_list_type
        )
      )
    }
    val str: String = prefix + getAdjListPrefix(adj_list_type) + "vertex_count"
    return str
  }

  /**
   * Get the path prefix of the edge num file path
   *
   * @param adj_list_type
   *   type of adj list structure.
   * @return
   *   the edge num file path. If edge info not support the adj list type, raise
   *   an IllegalArgumentException error.
   */
  def getEdgesNumPathPrefix(adj_list_type: AdjListType.Value): String = {
    if (containAdjList(adj_list_type) == false) {
      throw new IllegalArgumentException(
        "adj list type not found: " + AdjListType.AdjListTypeToString(
          adj_list_type
        )
      )
    }
    val str: String = prefix + getAdjListPrefix(adj_list_type) + "edge_count"
    return str
  }

  /**
   * Get the edge num file path of the vertex chunk
   *
   * @param chunk_index
   *   index of vertex chunk.
   * @param adj_list_type
   *   type of adj list structure.
   * @return
   *   the edge num file path. If edge info not support the adj list type, raise
   *   an IllegalArgumentException error.
   */
  def getEdgesNumFilePath(
      chunk_index: Long,
      adj_list_type: AdjListType.Value
  ): String = {
    if (containAdjList(adj_list_type) == false) {
      throw new IllegalArgumentException(
        "adj list type not found: " + AdjListType.AdjListTypeToString(
          adj_list_type
        )
      )
    }
    val str: String = prefix + getAdjListPrefix(adj_list_type) + "edge_count" +
      chunk_index.toString()
    return str
  }

  /**
   * Get the adj list offset chunk file path of vertex chunk the offset chunks
   * is aligned with the vertex chunks
   *
   * @param chunk_index
   *   index of vertex chunk.
   * @param adj_list_type
   *   type of adj list structure.
   * @return
   *   the offset chunk file path. If edge info not support the adj list type,
   *   raise an IllegalArgumentException error.
   */
  def getAdjListOffsetFilePath(
      chunk_index: Long,
      adj_list_type: AdjListType.Value
  ): String = {
    if (containAdjList(adj_list_type) == false) {
      throw new IllegalArgumentException(
        "adj list type not found: " + AdjListType.AdjListTypeToString(
          adj_list_type
        )
      )
    }
    val str: String =
      prefix + getAdjListPrefix(adj_list_type) + "offset/chunk" +
        chunk_index.toString()
    return str
  }

  /**
   * Get the path prefix of the adjacency list offset for the given adjacency
   * list type.
   * @param adj_list_type
   *   type of adj list structure.
   * @return
   *   the path prefix of the offset. If edge info not support the adj list
   *   type, raise an IllegalArgumentException error.
   */
  def getOffsetPathPrefix(adj_list_type: AdjListType.Value): String = {
    if (containAdjList(adj_list_type) == false) {
      throw new IllegalArgumentException(
        "adj list type not found: " + AdjListType.AdjListTypeToString(
          adj_list_type
        )
      )
    }
    return prefix + getAdjListPrefix(adj_list_type) + "offset/"
  }

  /**
   * Get the file path of adj list topology chunk.
   *
   * @param vertex_chunk_index
   *   index of vertex chunk.
   * @param chunk_index
   *   index of edge chunk.
   * @param adj_list_type
   *   type of adj list structure.
   * @return
   *   adj list chunk file path.
   */
  def getAdjListFilePath(
      vertex_chunk_index: Long,
      chunk_index: Long,
      adj_list_type: AdjListType.Value
  ): String = {
    var str: String =
      prefix + getAdjListPrefix(adj_list_type) + "adj_list/part" +
        vertex_chunk_index.toString() + "/chunk" + chunk_index.toString()
    return str
  }

  /**
   * Get the path prefix of adj list topology chunk of certain vertex chunk.
   *
   * @param vertex_chunk_index
   *   index of vertex chunk.
   * @param adj_list_type
   *   type of adj list structure.
   * @return
   *   path prefix of the edge chunk of vertices of given vertex chunk.
   */
  def getAdjListPathPrefix(
      vertex_chunk_index: Long,
      adj_list_type: AdjListType.Value
  ): String = {
    var str: String =
      prefix + getAdjListPrefix(adj_list_type) + "adj_list/part" +
        vertex_chunk_index.toString() + "/"
    return str
  }

  /**
   * Get the path prefix of the adjacency list topology chunk for the given
   * adjacency list type.
   * @param adj_list_type
   *   type of adj list structure.
   * @return
   *   path prefix of of the adjacency list topology.
   */
  def getAdjListPathPrefix(adj_list_type: AdjListType.Value): String = {
    return prefix + getAdjListPrefix(adj_list_type) + "adj_list/"
  }

  /**
   * Get the chunk file path of adj list property group. the property group
   * chunks is aligned with the adj list topology chunks
   *
   * @param property_group
   *   property group
   * @param adj_list_type
   *   type of adj list structure.
   * @param vertex_chunk_index
   *   index of vertex chunk.
   * @param chunk_index
   *   index of edge chunk.
   *
   * @return
   *   property group chunk file path. If edge info not contains the property
   *   group, raise an IllegalArgumentException error.
   */
  def getPropertyFilePath(
      property_group: PropertyGroup,
      adj_list_type: AdjListType.Value,
      vertex_chunk_index: Long,
      chunk_index: Long
  ): String = {
    if (containPropertyGroup(property_group) == false)
      throw new IllegalArgumentException("property group not found.")
    var str: String = property_group.getPrefix
    if (str == "") {
      val properties = property_group.getProperties
      val num = properties.size
      for (j <- 0 to num - 1) {
        if (j > 0)
          str += GeneralParams.regularSeparator
        str += properties.get(j).getName;
      }
      str += "/"
    }
    str = prefix + getAdjListPrefix(adj_list_type) + str + "part" +
      vertex_chunk_index.toString() + "/chunk" + chunk_index.toString()
    return str
  }

  /**
   * Get path prefix of adj list property group of certain vertex chunk.
   *
   * @param property_group
   *   property group.
   * @param adj_list_type
   *   type of adj list structure.
   * @param vertex_chunk_index
   *   index of vertex chunk.
   * @return
   *   path prefix of property group chunks of of vertices of given vertex
   *   chunk. If edge info not contains the property group, raise an
   *   IllegalArgumentException error.
   */
  def getPropertyGroupPathPrefix(
      property_group: PropertyGroup,
      adj_list_type: AdjListType.Value,
      vertex_chunk_index: Long
  ): String = {
    if (containPropertyGroup(property_group) == false)
      throw new IllegalArgumentException("property group not found.")
    var str: String = property_group.getPrefix
    if (str == "") {
      val properties = property_group.getProperties
      val num = properties.size
      for (j <- 0 to num - 1) {
        if (j > 0)
          str += GeneralParams.regularSeparator
        str += properties.get(j).getName;
      }
      str += "/"
    }
    str = prefix + getAdjListPrefix(adj_list_type) + str + "part" +
      vertex_chunk_index.toString() + "/"
    return str
  }

  /**
   * Get the path prefix of the property group chunk for the given adjacency
   * list type
   * @param property_group
   *   property group.
   * @param adj_list_type
   *   type of adj list structure.
   * @return
   *   path prefix of property group chunks. If edge info not contains the
   *   property group, raise an IllegalArgumentException error.
   */
  def getPropertyGroupPathPrefix(
      property_group: PropertyGroup,
      adj_list_type: AdjListType.Value
  ): String = {
    if (containPropertyGroup(property_group) == false)
      throw new IllegalArgumentException("property group not found.")
    var str: String = property_group.getPrefix
    if (str == "") {
      val properties = property_group.getProperties
      val num = properties.size
      for (j <- 0 to num - 1) {
        if (j > 0)
          str += GeneralParams.regularSeparator
        str += properties.get(j).getName;
      }
      str += "/"
    }
    str = prefix + getAdjListPrefix(adj_list_type) + str
    return str
  }

  def getConcatKey(): String = {
    return getSrc_label + GeneralParams.regularSeparator + getEdge_label + GeneralParams.regularSeparator + getDst_label
  }

  /** Dump to Yaml string. */
  def dump(): String = {
    val data = new java.util.HashMap[String, Object]()
    data.put("src_label", src_label)
    data.put("edge_label", edge_label)
    data.put("dst_label", dst_label)
    data.put("chunk_size", new java.lang.Long(chunk_size))
    data.put("src_chunk_size", new java.lang.Long(src_chunk_size))
    data.put("dst_chunk_size", new java.lang.Long(dst_chunk_size))
    if (prefix != "") data.put("prefix", prefix)
    data.put("version", version)
    val adj_list_num = adj_lists.size()
    if (adj_list_num > 0) {
      val adj_list_maps = new java.util.ArrayList[Object]()
      for (i <- 0 until adj_list_num) {
        adj_list_maps.add(adj_lists.get(i).toMap())
      }
      data.put("adj_lists", adj_list_maps)
    }
    val property_group_num = property_groups.size()
    if (property_group_num > 0) {
      val property_group_maps = new java.util.ArrayList[Object]()
      for (i <- 0 until property_group_num) {
        property_group_maps.add(property_groups.get(i).toMap())
      }
      data.put("property_groups", property_group_maps)
    }
    val options = new DumperOptions()
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK)
    options.setIndent(4)
    options.setIndicatorIndent(2);
    options.setPrettyFlow(true)
    val yaml = new Yaml(options)
    return yaml.dump(data)
  }
}

/** Helper object to load edge info files */
object EdgeInfo {

  /** Load a yaml file from path and construct a EdgeInfo from it. */
  def loadEdgeInfo(edgeInfoPath: String, spark: SparkSession): EdgeInfo = {
    val path = new Path(edgeInfoPath)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val input = fs.open(path)
    val yaml = new Yaml(new Constructor(classOf[EdgeInfo], new LoaderOptions()))
    return yaml.load(input).asInstanceOf[EdgeInfo]
  }
}
