# GraphAr Info

The **graphar-info** module is part of the pure Java implementation of GraphAr.

## Key Features

The **java-info** module offers a lightweight, zero-dependencies yet powerful solution for **GraphAr metadata management**. It is responsible for:

* **Loading & Saving Metadata**: Read and write GraphAr metadata files in YAML format.
* **Schema Parsing**: Extract graph schema definitions (e.g., vertex/edge types, properties).
* **Storage Information Retrieval**: Obtain detailed storage information from metadata, including file URIs, chunk sizes, etc.

## Building

To build the graphar-info module, you need:

- JDK 11 or higher
- Maven 3.5 or higher

Build the module with Maven:

```bash
mvn clean install
```

## Usage

Here's a simple example of how to use the graphar-info module:

For more usage examples and detailed information, please refer to [Getting Started with Info Module](https://graphar.apache.org/docs/libraries/java/info/getting-started).

```java
import org.apache.graphar.info.GraphInfo;
import org.apache.graphar.info.loader.GraphInfoLoader;
import org.apache.graphar.info.loader.impl.LocalFileSystemStreamGraphInfoLoader;

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
## Dependencies

The graphar-info module has minimal dependencies:
- SnakeYAML for YAML parsing (shaded in the JAR)

**Under active development**