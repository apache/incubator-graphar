---
id: getting-started
title: Getting Started with Info Module
sidebar_position: 1
---

# Getting Started with Info Module

This article is a quick guide that explains how to work with GraphAr Java Info module. The Info module is part of the pure Java implementation of GraphAr and provides capabilities for parsing graph metadata (schema) from YAML files.

## metadata Files

GraphAr uses a group of Yaml files to save the metadata for a graph.

### GraphInfo

The graphInfo file defines the most basic metadata of a graph including its name, the root directory path of the data files, the vertex metadata and edge metadata files it contains, and the version of GraphAr. For example, the file "ldbc_sample.graph.yml" defines an example graph named "ldbc_sample", which includes one type of vertices ("person") and one type of edges ("person knows person").

```yaml
name: ldbc_sample
prefix: ./
vertices:
  - person.vertex.yml
edges:
  - person_knows_person.edge.yml
version: gar/v1
```

### VertexInfo

Each vertexInfo file defines the metadata of a type of vertex, e.g., "person" in this case. The vertex chunk size, the path for vertex data files and the version of GraphAr are specified. These vertices could have some properties, which are divided into property groups. Each property group has its own file type (CSV, ORC or Parquet) and the prefix of the path for its data files, it also lists all properties in this group, with every property contains the name, data type and if it is the primary key.

An example of the vertex metadata file is shown as follows:

```yaml
type: person
chunk_size: 100
prefix: vertex/person/
property_groups:
  - properties:
      - name: id
        data_type: int64
        is_primary: true
    file_type: parquet
  - properties:
      - name: firstName
        data_type: string
        is_primary: false
      - name: lastName
        data_type: string
        is_primary: false
      - name: gender
        data_type: string
        is_primary: false
    file_type: parquet
version: gar/v1
```

### EdgeInfo

Each edge metadata file defines a single type of edges with specific types for the source vertex, destination vertex and the edge, e.g., "person_knows_person" in this case. It defines the metadata such as the edge chunk size, the source vertex chunk size, the destination vertex chunk size, if the edges are directed or not, the relative file path for edge data files, the adjLists and the version of GraphAr.

An example of the edge metadata file is shown as follows:

```yaml
src_type: person
edge_type: knows
dst_type: person
chunk_size: 1024
src_chunk_size: 100
dst_chunk_size: 100
directed: false
prefix: edge/person_knows_person/
adj_lists:
  - ordered: false
    aligned_by: src
    file_type: parquet
  - ordered: true
    aligned_by: src
    file_type: parquet
  - ordered: true
    aligned_by: dst
    file_type: parquet
property_groups:
  - file_type: parquet
    properties:
      - name: creationDate
        data_type: string
        is_primary: false
version: gar/v1
```

In GraphAr format, separate data files are used to store the structure (called adjList) and the properties for edges. The adjList type can be either of **unordered_by_source**, **unordered_by_dest**, **ordered_by_source** or **ordered_by_dest**. For a specific type of adjList, the metadata includes its file path prefix, the file type, as well as all the property groups attached.

## How to Use GraphAr Java Info Module

### Load graph Info

#### Load GraphInfo from local file system

Here's a simple example of how to use the graphar-info module:

```java
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.loader.GraphInfoLoader;
import org.apache.graphar.info.loader.impl.LocalFileSystemStreamGraphInfoLoader;
import java.nio.file.Paths;

// Load graph info from a YAML file
GraphInfoLoader loader = new LocalFileSystemStreamGraphInfoLoader();
GraphInfo graphInfo = loader.loadGraphInfo(Paths.get("path/to/graph.yml").toUri());

// Access graph metadata
String graphName = graphInfo.getName();
List<VertexInfo> vertices = graphInfo.getVertexInfos();
List<EdgeInfo> edges = graphInfo.getEdgeInfos();

// Access vertex metadata
VertexInfo personVertex = vertices.get(0);
String vertexType = personVertex.getType();
long chunkSize = personVertex.getChunkSize();

// Access edge metadata
EdgeInfo knowsEdge = edges.get(0);
String edgeType = knowsEdge.getEdgeType();
boolean isDirected = knowsEdge.isDirected();
```

#### Custom YAML Loader Implementation

The java-info module requires users to implement their own YAML reading interface, because we typically face data lakes where data may be stored anywhere (local file or HDFS or S3 or OSS...). Users can implement one or more of StringGraphInfoLoader, ReaderGraphInfoLoader, or StreamGraphInfoLoader (a local file system implementation is already provided).

Here's an example of how to implement a custom YAML loader by extending the StringGraphInfoLoader abstract class:

```java
import org.apache.graphar.info.loader.StringGraphInfoLoader;
import java.io.IOException;
import java.net.URI;

public class MyStringGraphInfoLoader extends StringGraphInfoLoader {
    @Override
    public String readYaml(URI uri) throws IOException {
        // Implement your custom logic to read YAML from any source
        // This example shows reading from a database, but it could be HTTP, S3, HDFS, etc.
        
        // Example: Read from a database based on URI path
        String path = uri.getPath();
        // Query database for YAML content
        return readYamlFromDatabase(path);
    }
    
    private String readYamlFromDatabase(String path) throws IOException {
        // Your database access logic here
        // This is just a placeholder implementation
        // In a real implementation, you would connect to your database
        // and retrieve the YAML content based on the path
        
        // For example:
        // Connection conn = DriverManager.getConnection(dbUrl, username, password);
        // PreparedStatement stmt = conn.prepareStatement("SELECT yaml_content FROM graphs WHERE id = ?");
        // stmt.setString(1, path);
        // ResultSet rs = stmt.executeQuery();
        // if (rs.next()) {
        //     return rs.getString("yaml_content");
        // }
        
        // Placeholder return
        return "name: example\ntype: graph\nversion: v1";
    }
}

// Usage of the custom loader
MyStringGraphInfoLoader customLoader = new MyStringGraphInfoLoader();
GraphInfo graphInfo = customLoader.loadGraphInfo(URI.create("db://mydatabase/graphs/graph1/graph.yml"));
```

### Building

To build the graphar-info module, you need:

- JDK 11 or higher
- Maven 3.5 or higher

Build the module with Maven:

```bash
cd maven-projects/info
mvn clean install
```
