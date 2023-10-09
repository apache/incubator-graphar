/**
 * Copyright 2022 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.alibaba.graphar

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{SparkSession}
import org.yaml.snakeyaml.{Yaml, DumperOptions}
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty

/** VertexInfo is a class to store the vertex meta information. */
class VertexInfo() {
  @BeanProperty var label: String = ""
  @BeanProperty var chunk_size: Long = 0
  @BeanProperty var prefix: String = ""
  @BeanProperty var property_groups = new java.util.ArrayList[PropertyGroup]()
  @BeanProperty var version: String = ""

  /**
   * Check if the vertex info contains the property group.
   *
   * @param property_group
   *   the property group to check.
   * @return
   *   true if the vertex info contains the property group, otherwise false.
   */
  def containPropertyGroup(property_group: PropertyGroup): Boolean = {
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
   * Check if the vertex info contains certain property.
   *
   * @param property_name
   *   name of the property.
   * @return
   *   true if the vertex info contains the property, otherwise false.
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
  def getPropertyGroup(property_name: String): PropertyGroup = {
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
    throw new IllegalArgumentException
  }

  /**
   * Get the data type of property.
   *
   * @param property_name
   *   name of the property.
   * @return
   *   the data type in gar of the property. If the vertex info does not
   *   contains the property, raise IllegalArgumentException error.
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
    throw new IllegalArgumentException
  }

  /**
   * Check if the property is primary key.
   *
   * @param property_name
   *   name of the property to check.
   * @return
   *   true if the property if the primary key of vertex info, otherwise return
   *   false.
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
    throw new IllegalArgumentException
  }

  /**
   * Get primary key of vertex info.
   *
   * @return
   *   name of the primary key.
   */
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

  /**
   * Check if the vertex info is validated.
   *
   * @return
   *   true if the vertex info is validated, otherwise return false.
   */
  def isValidated(): Boolean = {
    if (label == "" || chunk_size <= 0) {
      return false
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

  /** Get the vertex num file path of vertex info. */
  def getVerticesNumFilePath(): String = {
    return prefix + "vertex_count"
  }

  /**
   * Get the chunk file path of property group of vertex chunk.
   *
   * @param property_group
   *   the property group.
   * @param chunk_index
   *   the index of vertex chunk
   * @return
   *   chunk file path.
   */
  def getFilePath(property_group: PropertyGroup, chunk_index: Long): String = {
    if (containPropertyGroup(property_group) == false) {
      throw new IllegalArgumentException
    }
    var str: String = ""
    if (property_group.getPrefix == "") {
      val properties = property_group.getProperties
      val num = properties.size
      for (j <- 0 to num - 1) {
        if (j > 0) {
          str += GeneralParams.regularSeperator
        }
        str += properties.get(j).getName;
      }
      str += "/"
    } else {
      str = property_group.getPrefix
    }
    return prefix + str + "chunk" + chunk_index.toString()
  }

  /**
   * Get the path prefix for the specified property group.
   *
   * @param property_group
   *   the property group.
   * @return
   *   the path prefix of the property group chunk files.
   */
  def getPathPrefix(property_group: PropertyGroup): String = {
    if (containPropertyGroup(property_group) == false) {
      throw new IllegalArgumentException
    }
    var str: String = ""
    if (property_group.getPrefix == "") {
      val properties = property_group.getProperties
      val num = properties.size
      for (j <- 0 to num - 1) {
        if (j > 0)
          str += GeneralParams.regularSeperator
        str += properties.get(j).getName;
      }
      str += "/"
    } else {
      str = property_group.getPrefix
    }
    return prefix + str
  }

  /** Dump to Yaml string. */
  def dump(): String = {
    val data = new java.util.HashMap[String, Object]()
    data.put("label", label)
    data.put("chunk_size", new java.lang.Long(chunk_size))
    if (prefix != "") data.put("prefix", prefix)
    data.put("version", version)
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

/** Helper object to load vertex info files */
object VertexInfo {

  /** Load a yaml file from path and construct a VertexInfo from it. */
  def loadVertexInfo(
      vertexInfoPath: String,
      spark: SparkSession
  ): VertexInfo = {
    val path = new Path(vertexInfoPath)
    val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
    val input = fs.open(path)
    val yaml = new Yaml(new Constructor(classOf[VertexInfo]))
    return yaml.load(input).asInstanceOf[VertexInfo]
  }
}
