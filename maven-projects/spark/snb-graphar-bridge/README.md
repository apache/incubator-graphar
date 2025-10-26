# LDBC SNB to GraphAr Bridge

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

This module provides a direct memory pipeline for converting LDBC Social Network Benchmark (SNB) data to Apache GraphAr format.

## Design

**Dual-Track Architecture**:
- **Static entities** (Person, Place, Organisation, Tag, TagClass): RDD-based batch processing from LDBC dictionaries
- **Dynamic entities** (Forum, Post, Comment): Streaming architecture processing LDBC activity serializer output

**Key Features**:
- Direct memory pipeline eliminates intermediate CSV file I/O
- Supports all 31 LDBC SNB entity types (8 vertices + 23 edges)
- Fully compliant with GraphAr v1.0 standard
- Batch processing with configurable chunk sizes

## Building

### Prerequisites

- Java 8 or Java 11
- Maven 3.6+
- SBT 1.x (for LDBC dependency)
- Apache Spark 3.5.1

### Build LDBC SNB Datagen Dependency

```bash
cd ../ldbc-snb-datagen
sbt assembly
```

### Build This Module

**IMPORTANT**: Build from project root, not from this directory.

```bash
cd /path/to/incubator-graphar/maven-projects

# Build with dependencies
mvn clean install -DskipTests \
  -pl spark/graphar,spark/datasources-35,spark/snb-graphar-bridge -am
```

## Usage

### Quick Start

```bash
spark-submit \
  --class org.apache.graphar.datasources.ldbc.examples.LdbcEnhancedBridgeExample \
  --master "local[*]" \
  --jars ../ldbc-snb-datagen/target/ldbc_snb_datagen_2.12_spark3.2-0.5.1+23-1d60a657-jar-with-dependencies.jar \
  target/snb-graphar-bridge-0.13.0-SNAPSHOT.jar \
  0.1 /tmp/graphar_output ldbc_test 256 256 parquet
```

### Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| scaleFactor | LDBC scale factor | `0.1` |
| outputPath | Output directory | `/tmp/graphar_output` |
| graphName | Graph identifier | `ldbc_test` |
| vertexChunkSize | Vertex chunk size | `256` |
| edgeChunkSize | Edge chunk size | `256` |
| fileType | File format | `parquet` |

**Scale Factor Guidelines**: Use SF≥0.1 for full testing (SF0.003 has limited dynamic entities).

### Output Structure

```
/tmp/graphar_output/
├── ldbc_test.graph.yml
├── vertex/
│   ├── Person/
│   ├── Organisation/
│   ├── Place/
│   ├── Tag/
│   ├── TagClass/
│   ├── Forum/
│   ├── Post/
│   └── Comment/
└── edge/
    ├── Person_knows_Person/
    ├── Person_hasInterest_Tag/
    ├── Forum_hasMember_Person/
    ├── Post_hasCreator_Person/
    └── ... (23 edge types total)
```

### Validation

```bash
spark-submit \
  --class org.apache.graphar.datasources.ldbc.examples.ValidateGraphArOutput \
  --master "local[*]" \
  target/snb-graphar-bridge-0.13.0-SNAPSHOT.jar \
  /tmp/graphar_output
```

## Testing

### Prerequisites

Build and install all dependency modules first:

```bash
cd /path/to/incubator-graphar/maven-projects

# Install dependencies
mvn clean install -DskipTests \
  -pl spark/graphar,spark/datasources-35,spark/snb-graphar-bridge -am

# Build LDBC dependency
cd spark/ldbc-snb-datagen
sbt assembly
```

### Run Tests

```bash
# Use Java 8 (recommended for local testing)
export JAVA_HOME=/opt/jdk1.8
export PATH=$JAVA_HOME/bin:$PATH

# Run all tests
cd /path/to/incubator-graphar/maven-projects
mvn test -pl spark/snb-graphar-bridge -am
```

### Test Suite

- `LdbcGraphArBridgeTest`: Core conversion functionality
- `PersonProcessorTest`: Person entity generation
- `OutputValidationTest`: GraphAr output structure validation
- `SchemaValidationTest`: Data integrity and schema validation (15 checks)

**Expected**: 4 tests, 0 failures, ~10-12 minutes execution time

### Common Issues

**Problem**: Dependency resolution errors
```
[ERROR] Failed to read artifact descriptor for org.apache.graphar:graphar-commons
```
**Solution**: Build from project root with `-am` flag (see Prerequisites)

---

**Problem**: Java module access errors (Java 17+)
```
java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ cannot access class sun.nio.ch.DirectBuffer
```
**Solution**: Use Java 8, or Java 11 with `--add-opens` flags:
```bash
export JAVA_HOME=/opt/jdk11
export JDK_JAVA_OPTIONS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED"
export MAVEN_OPTS="-Xmx3g -Xms512m"
```

---

**Problem**: Out of memory errors
```
java.lang.OutOfMemoryError: Java heap space
```
**Solution**: Memory limits configured in pom.xml (-Xmx2g). Increase if needed:
```bash
export MAVEN_OPTS="-Xmx4g -Xms1g"
```

## License

Licensed under the Apache License, Version 2.0. See the LICENSE and NOTICE files for details.

This module uses LDBC SNB Datagen (https://github.com/ldbc/ldbc_snb_datagen), licensed under Apache License 2.0.
