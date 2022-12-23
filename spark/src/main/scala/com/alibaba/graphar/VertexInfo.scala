package com.alibaba.graphar

import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty

class VertexInfo() {
  @BeanProperty var label: String = ""
  @BeanProperty var chunk_size: Long = 0
  @BeanProperty var prefix: String = ""
  @BeanProperty var property_groups = new java.util.ArrayList[PropertyGroup]()
  @BeanProperty var version: String = ""

  def containPropertyGroup(property_group: PropertyGroup): Boolean = {
    val len: Int = property_groups.size
    for ( i <- 0 to len - 1 ) {
      val pg: PropertyGroup = property_groups.get(i)
      if (pg == property_group) {
        return true
      }
    }
    return false
  }

  def containProperty(property_name: String): Boolean = {
    val len: Int = property_groups.size
    for ( i <- 0 to len - 1 ) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      for ( j <- 0 to num - 1 ) {
        if (properties.get(j).getName == property_name) {
          return true
        }
      }
    }
    return false
  }

  def getPropertyGroup(property_name: String): PropertyGroup = {
    val len: Int = property_groups.size
    for ( i <- 0 to len - 1 ) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      for ( j <- 0 to num - 1 ) {
        if (properties.get(j).getName == property_name) {
          return pg
        }
      }
    }
    throw new IllegalArgumentException
  }

  def getPropertyType(property_name: String): GarType.Value = {
    val len: Int = property_groups.size
    for ( i <- 0 to len - 1 ) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      for ( j <- 0 to num - 1 ) {
        if (properties.get(j).getName == property_name) {
          return properties.get(j).getData_type_in_gar
        }
      }
    }
    throw new IllegalArgumentException
  }

  def isPrimaryKey(property_name: String): Boolean = {
    val len: Int = property_groups.size
    for ( i <- 0 to len - 1 ) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      for ( j <- 0 to num - 1 ) {
        if (properties.get(j).getName == property_name) {
          return properties.get(j).getIs_primary
        }
      }
    }
    throw new IllegalArgumentException
  }

  def getPrimaryKey(): String = {
    val len: Int = property_groups.size
    for ( i <- 0 to len - 1 ) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      for ( j <- 0 to num - 1 ) {
        if (properties.get(j).getIs_primary) {
          return properties.get(j).getName
        }
      }
    }
    return ""
  }

 def isValidated(): Boolean = {
    if (label == "" || chunk_size <= 0)
      return false
    val len: Int = property_groups.size
    for ( i <- 0 to len - 1 ) {
      val pg: PropertyGroup = property_groups.get(i)
      val properties = pg.getProperties
      val num = properties.size
      if (num == 0)
        return false
      val file_type = pg.getFile_type_in_gar
    }
    return true
  }

  def getVerticesNumFilePath(): String = {
    return prefix + "vertex_count"
  }

  def getFilePath(property_group: PropertyGroup, chunk_index: Long): String = {
    if (containPropertyGroup(property_group) == false)
      throw new IllegalArgumentException
    var str: String = ""
    if (property_group.getPrefix == "") {
      val properties = property_group.getProperties
      val num = properties.size
      for ( j <- 0 to num - 1 ) {
        if (j > 0)
          str += GeneralParams.regularSeperator
        str += properties.get(j).getName;
      }
      str += "/"
    } else {
      str = property_group.getPrefix
    }
    return prefix + str + "part" + chunk_index.toString() + "/chunk0"
  }

  def getDirPath(property_group: PropertyGroup): String = {
    if (containPropertyGroup(property_group) == false)
      throw new IllegalArgumentException
    var str: String = ""
    if (property_group.getPrefix == "") {
      val properties = property_group.getProperties
      val num = properties.size
      for ( j <- 0 to num - 1 ) {
        if (j > 0)
          str += GeneralParams.regularSeperator
        str += properties.get(j).getName;
      }
      str += "/"
    } else {
      str = property_group.getPrefix
    }
    return prefix + str;
  }
}
