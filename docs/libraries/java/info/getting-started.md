---
id: getting-started
title: Getting Started with Info Module
sidebar_position: 1
---

# Getting Started with Info Module

This article is a quick guide that explains how to work with GraphAr Java Info module. The Info module is part of the pure Java implementation of GraphAr and provides capabilities for parsing graph metadata (schema) from YAML files.

## metadata Files

GraphAr uses a group of information files to save the metadata for a graph. For more information, see [GraphAr Format Specification](https://graphar.apache.org/docs/specification/format#information-files).

`java-info` module provides the function of reading and parsing information files

## How to Use GraphAr Java Info Module

### Load graph Info

#### Load GraphInfo from local file system

Here's a simple example of how to use the java-info module:

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

The java-info module requires users to implement their own YAML reading interface, because we typically face data lakes where data may be stored anywhere (local file or HDFS or S3 or OSS...). Users can implement one or more of `StringGraphInfoLoader`, `ReaderGraphInfoLoader`, or `StreamGraphInfoLoader` (a local file system implementation is already provided).

Here's an example of how to implement a custom YAML loader by extending the `StringGraphInfoLoader` abstract class:

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

### Save Graph Info

The java-info module also provides functionality to save graph metadata to YAML files using the `GraphSaver` interface. Here's an example of how to use it:

```java
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.saver.GraphInfoSaver;
import org.apache.graphar.info.saver.impl.LocalFileSystemYamlGraphSaver;
import java.net.URI;

// Create or obtain a GraphInfo object
GraphInfo graphInfo = createOrLoadGraphInfo(); // your method to create or load GraphInfo

// Create a GraphSaver instance
GraphInfoSaver graphSaver = new LocalFileSystemYamlGraphSaver();

// Save the graph info to a directory
String savePath = "/path/to/save/directory";
try {
    graphSaver.save(URI.create(savePath), graphInfo);
    System.out.println("Graph info saved successfully to " + savePath);
} catch (IOException e) {
    System.err.println("Failed to save graph info: " + e.getMessage());
    e.printStackTrace();
}
```

This will save the graph metadata as a set of YAML files:
- One main graph YAML file (e.g., `graph-name.graph.yaml`)
- One YAML file for each vertex type (e.g., `person.vertex.yaml`)
- One YAML file for each edge type (e.g., `person_knows_person.edge.yaml`)

Alternatively, you can use the dump method to convert graph info to a string and store it anywhere:

```java
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.EdgeInfo;
import org.apache.graphar.info.VertexInfo;
import java.net.URI;

// Create or obtain a GraphInfo object
GraphInfo graphInfo = createOrLoadGraphInfo(); // your method to create or load GraphInfo

// Set custom storage URIs for vertex and edge info files
for (VertexInfo vertexInfo : graphInfo.getVertexInfos()) {
    graphInfo.setStoreUri(vertexInfo, URI.create("db://path/vertex/" + vertexInfo.getType() + ".vertex.yaml"));
}

for (EdgeInfo edgeInfo : graphInfo.getEdgeInfos()) {
    graphInfo.setStoreUri(edgeInfo, URI.create("db://path/edge/" + edgeInfo.getConcat() + ".edge.yaml"));
}

// Convert graph info to YAML string
String graphYamlString = graphInfo.dump();

// Now you can store the YAML string anywhere you want
// For example, save to a database, send over network, etc.
saveYamlStringToDatabase(graphYamlString);
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