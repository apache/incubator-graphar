package com.alibaba.graphar

import java.io.{File, FileInputStream}
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import scala.beans.BeanProperty

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
  @BeanProperty var version: String = ""

  def containAdjList(adj_list_type: AdjListType.Value): Boolean = {
    val tot: Int = adj_lists.size
    for ( k <- 0 to tot - 1 ) {
      val adj_list = adj_lists.get(k)
      if (adj_list.getAdjList_type_in_gar == adj_list_type)
        return true
    }
    return false
  }

  def getAdjListPrefix(adj_list_type: AdjListType.Value): String = {
    val tot: Int = adj_lists.size
    for ( k <- 0 to tot - 1 ) {
      val adj_list = adj_lists.get(k)
      if (adj_list.getAdjList_type_in_gar == adj_list_type) {
        var str: String = adj_list.getPrefix
        if (str == "") {
          str = AdjListType.AdjListTypeToString(adj_list_type) + "/"
        }
        return str
      }
    }
    throw new IllegalArgumentException
  }

  def getAdjListFileType(adj_list_type: AdjListType.Value): FileType.Value = {
    val tot: Int = adj_lists.size
    for ( k <- 0 to tot - 1 ) {
      val adj_list = adj_lists.get(k)
      if (adj_list.getAdjList_type_in_gar == adj_list_type) {
        return adj_list.getFile_type_in_gar
      }
    }
    throw new IllegalArgumentException
  }

  def getPropertyGroups(adj_list_type: AdjListType.Value): java.util.ArrayList[PropertyGroup] = {
    val tot: Int = adj_lists.size
    for ( k <- 0 to tot - 1 ) {
      val adj_list = adj_lists.get(k)
      if (adj_list.getAdjList_type_in_gar == adj_list_type) {
        return adj_list.getProperty_groups
      }
    }
    throw new IllegalArgumentException
  }

  def containPropertyGroup(property_group: PropertyGroup, adj_list_type: AdjListType.Value): Boolean = {
    val tot: Int = adj_lists.size
    for ( k <- 0 to tot - 1 ) {
      val adj_list = adj_lists.get(k)
      if (adj_list.getAdjList_type_in_gar == adj_list_type) {
        val property_groups = adj_list.getProperty_groups
        val len: Int = property_groups.size
        for ( i <- 0 to len - 1 ) {
          val pg: PropertyGroup = property_groups.get(i)
          if (pg == property_group) {
            return true
          }
        }
      }
    }
    return false
  }

  def containProperty(property_name: String): Boolean = {
    val tot: Int = adj_lists.size
    for ( k <- 0 to tot - 1 ) {
      val adj_list = adj_lists.get(k)
      val property_groups = adj_list.getProperty_groups
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
    }
    return false
  }

  def getPropertyGroup(property_name: String, adj_list_type: AdjListType.Value): PropertyGroup = {
    val tot: Int = adj_lists.size
    for ( k <- 0 to tot - 1 ) {
      val adj_list = adj_lists.get(k)
      if (adj_list.getAdjList_type_in_gar == adj_list_type) {
        val property_groups = adj_list.getProperty_groups
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
      }
    }
    throw new IllegalArgumentException
  }

  def getPropertyType(property_name: String): GarType.Value = {
    val tot: Int = adj_lists.size
    for ( k <- 0 to tot - 1 ) {
      val adj_list = adj_lists.get(k)
      val property_groups = adj_list.getProperty_groups
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
    }
    throw new IllegalArgumentException
  }

  def isPrimaryKey(property_name: String): Boolean = {
    val tot: Int = adj_lists.size
    for ( k <- 0 to tot - 1 ) {
      val adj_list = adj_lists.get(k)
      val property_groups = adj_list.getProperty_groups
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
    }
    throw new IllegalArgumentException
  }

  def getPrimaryKey(): String = {
    val tot: Int = adj_lists.size
    for ( k <- 0 to tot - 1 ) {
      val adj_list = adj_lists.get(k)
      val property_groups = adj_list.getProperty_groups
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
    }
    return ""
  }

  def isValidated(): Boolean = {
    if (src_label == "" || edge_label == "" || dst_label == "")
      return false
    if (chunk_size <= 0 || src_chunk_size <= 0 || dst_chunk_size <= 0)
      return false
    val tot: Int = adj_lists.size
    for ( k <- 0 to tot - 1 ) {
      val adj_list = adj_lists.get(k)
      val file_type = adj_list.getFile_type_in_gar
      val property_groups = adj_list.getProperty_groups
      val len: Int = property_groups.size
      for ( i <- 0 to len - 1 ) {
        val pg: PropertyGroup = property_groups.get(i)
        val properties = pg.getProperties
        val num = properties.size
        if (num == 0)
          return false
        val pg_file_type = pg.getFile_type_in_gar
      }
    }
    return true
  }

  def getAdjListOffsetFilePath(chunk_index: Long, adj_list_type: AdjListType.Value) : String = {
    if (containAdjList(adj_list_type) == false)
      throw new IllegalArgumentException
    val str: String = prefix + getAdjListPrefix(adj_list_type) + "offset/part" +
           chunk_index.toString() + "/chunk0"
    return str
  }

  def getAdjListOffsetDirPath(adj_list_type: AdjListType.Value) : String = {
    if (containAdjList(adj_list_type) == false)
      throw new IllegalArgumentException
    return prefix + getAdjListPrefix(adj_list_type) + "offset/"
  }

  def getAdjListFilePath(vertex_chunk_index: Long, chunk_index: Long, adj_list_type: AdjListType.Value) : String = {
    var str: String = prefix + getAdjListPrefix(adj_list_type) + "adj_list/part" +
        vertex_chunk_index.toString() + "/chunk" + chunk_index.toString()
    return str
  }

  def getAdjListDirPath(adj_list_type: AdjListType.Value) : String = {
    return prefix + getAdjListPrefix(adj_list_type) + "adj_list/"
  }

  def getPropertyFilePath(property_group: PropertyGroup, adj_list_type: AdjListType.Value, vertex_chunk_index: Long, chunk_index: Long) : String = {
    if (containPropertyGroup(property_group, adj_list_type) == false)
      throw new IllegalArgumentException
    var str: String = property_group.getPrefix
    if (str == "") {
      val properties = property_group.getProperties
      val num = properties.size
      for ( j <- 0 to num - 1 ) {
        if (j > 0)
          str += GeneralParams.regularSeperator
        str += properties.get(j).getName;
      }
      str +=  "/"
    }
    str = prefix + getAdjListPrefix(adj_list_type) + str + "part" +
        vertex_chunk_index.toString() + "/chunk" + chunk_index.toString()
    return str
  }

  def getPropertyDirPath(property_group: PropertyGroup, adj_list_type: AdjListType.Value) : String = {
    if (containPropertyGroup(property_group, adj_list_type) == false)
      throw new IllegalArgumentException
    var str: String = property_group.getPrefix
    if (str == "") {
      val properties = property_group.getProperties
      val num = properties.size
      for ( j <- 0 to num - 1 ) {
        if (j > 0)
          str += GeneralParams.regularSeperator
        str += properties.get(j).getName;
      }
      str +=  "/"
    }
    str = prefix + getAdjListPrefix(adj_list_type) + str
    return str
  }
}
