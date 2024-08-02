# GraphAr Spark

This directory contains the code and build system for the GraphAr Spark library.

## Building GraphAr Spark

### System setup

GraphAr Spark uses maven as a package build system.

Building requires:

- JDK 8 or JDK 11
- Maven 3.2.0 or higher

### Building

All the instructions below assume that you have cloned the GraphAr git
repository and navigated to the ``spark`` subdirectory:

```bash
    $ git clone https://github.com/apache/incubator-graphar.git
    $ cd incubator-graphar
    $ cd maven-projects/spark
```


Build the package:

```bash
    $ mvn clean install -DskipTests
```

GraphAr Spark uses Maven Profiles to support multiple Spark Versions. By default it is built with Spark 3.2.x or profile `datasources-32`. To built with Spark 3.3.4 use `-P datasources-33` (`mvn clean install -DskipTests -P datasources-33`).

After compilation, the package file graphar-x.x.x-SNAPSHOT-shaded.jar is generated in the directory ``spark/graphar/target/``.

Build the package and run the unit tests:

first, you need to download the testing data:

```bash
    $ git clone https://github.com/apache/incubator-graphar-testing.git testing
```

```bash
    $ GRA_TEST_DATA=./testing mvn clean install
```

Build and run the unit tests:

```bash
    $ GRA_TEST_DATA=./testing mvn clean test
```

Build and run certain unit test:

```bash
    $ GRA_TEST_DATA=${PWD}/testing mvn clean test -Dsuites='org.apache.graphar.GraphInfoSuite'   # run the GraphInfo test suite
    $ GRA_TEST_DATA=${PWD}/testing mvn clean test -Dsuites='org.apache.graphar.GraphInfoSuite load graph info'  # run the `load graph info` test of test suite
```

### Generate API document

Building the API document with maven:

```bash
    $ mvn scala:doc
```

The API document is generated in the directory ``spark/graphar/target/site/scaladocs``.

## Running Neo4j to GraphAr example

Spark provides a simple example to convert Neo4j data to GraphAr data.
The example is located in the directory ``spark/graphar/src/main/scala/org/apache/graphar/examples/``.

To run the example, download Spark and Neo4j first.

### Spark 3.2.x

Spark 3.2.x is the recommended runtime to use. The rest of the instructions are provided assuming Spark 3.2.x. Alternative supported Spark version is 3.3.4, to force building with it use Maven Profile `datasources-33`.

To place Spark under `${HOME}`:

```bash
scripts/get-spark-to-home.sh
export SPARK_HOME="${HOME}/spark-3.2.2-bin-hadoop3.2"
export PATH="${SPARK_HOME}/bin":"${PATH}"
```

### Neo4j 4.4.x

Neo4j 4.4.x is the LTS version to use. The rest of the instructions are provided assuming Neo4j 4.4.x.

Neo4j is required to have a pre-installed, compatible Java Virtual Machine (JVM). **For Neo4j 4.4.x, jdk11/jre11 is needed.**
Run `java --version` to check it.

To place Neo4j under `${HOME}`:

```bash
scripts/get-neo4j-to-home.sh
export NEO4J_HOME="${HOME}/neo4j-community-4.4.23"
export PATH="${NEO4J_HOME}/bin":"${PATH}"
# initialize the password for user database
neo4j-admin set-initial-password xxxx # set your password here
```

Start Neo4j server and load movie data:

```bash
scripts/deploy-neo4j-movie-data.sh
```

The username is ``neo4j`` and the password is the one you set in the previous step.
Open the Neo4j browser at http://localhost:7474/browser/ to check the movie graph data.

### Building the project

Run:

```bash
scripts/build.sh
```

### Running the Neo4j2GraphAr example

```bash
export NEO4J_USR="neo4j"
export NEO4J_PWD="xxxx" # the password you set in the previous step
scripts/run-neo4j2graphar.sh
```

The example will convert the movie data in Neo4j to GraphAr data and save it to the directory ``/tmp/graphar/neo4j2graphar``.

### Running the GraphAr2Neo4j example

We can also import the movie graph from GraphAr to Neo4j.

First clear the Neo4j movie graph to show the import result clearly:
```bash
echo "match (a) -[r] -> () delete a, r;match (a) delete a;" | cypher-shell -u ${NEO4J_USR} -p ${NEO4J_PWD} -d neo4j --format plain
```

Then run the example:

```bash
scripts/run-graphar2neo4j.sh
```

The example will import the movie graph from GraphAr to Neo4j and you can check the result in the Neo4j browser.

## Running self defined neo4j importer

We can write a json configuration file like `import/neo4j.json` to do the import. Here is an example.

1. Import movie data in neo4j
2. Fill in the neo4j connection fields in the json file.
3. `cd import`
4. `./neo4j.sh neo4j.json`

## Running NebulaGraph to GraphAr example

Running this example requires `Docker` to be installed, if not, follow [this link](https://docs.docker.com/engine/install/). Run `docker version` to check it.

Spark provides a simple example to convert NebulaGraph data to GraphAr data.
The example is located in the directory ``spark/src/main/scala/org/apache/graphar/examples/``.

To run the example, download Spark and Neo4j first.

### Spark 3.2.x

Spark 3.2.x is the recommended runtime to use. The rest of the instructions are provided assuming Spark 3.2.x.

To place Spark under `${HOME}`:

```bash
scripts/get-spark-to-home.sh
export SPARK_HOME="${HOME}/spark-3.2.2-bin-hadoop3.2"
export PATH="${SPARK_HOME}/bin":"${PATH}"
```

### NebulaGraph

To place NebulaGraph docker-compose.yaml under `${HOME}`:

```bash
scripts/get-nebula-to-home.sh
```

Start NebulaGraph server by Docker and load `basketballplayer` data:

```bash
scripts/deploy-nebula-default-data.sh
```

Use [NebulaGraph Studio](https://docs.nebula-graph.com.cn/master/nebula-studio/deploy-connect/st-ug-deploy/#docker_studio) to check the graph data, the username is ``root`` and the password is ``nebula``.

### Building the project

Run:

```bash
scripts/build.sh
```

### Running the Nebula2GraphAr example

```bash
scripts/run-nebula2graphar.sh
```

The example will convert the basketballplayer data in NebulaGraph to GraphAr data and save it to the directory ``/tmp/graphar/nebula2graphar``.

### Running the GraphAr2Nebula example

We can also import the basketballplayer graph from GraphAr to NebulaGraph.

First clear the NebulaGraph's basketballplayer graph space to show the import result clearly:

```bash
docker run \
    --rm \
    --name nebula-console-loader \
    --network nebula-docker-env_nebula-net \
    vesoft/nebula-console:nightly -addr 172.28.3.1 -port 9669 -u root -p nebula -e "use basketballplayer; clear space basketballplayer;"
```

Then run the example:

```bash
scripts/run-graphar2nebula.sh
```

The example will import the basketballplayer graph from GraphAr to NebulaGraph and you can check the result in NebulaGraph Studio.

## Running the local LDBC sample data to GraphAr example

we provide a simple example to convert LDBC sample data to GraphAr data.
this example is located in the directory ``spark/src/main/scala/org/apache/graphar/examples/``.

To run the example, first build the project:

```bash
scripts/build.sh
```

Then run the example:


```bash
# you first need to specify the `GAR_TEST_DATA` environment variable to the testing data directory:
export GAR_TEST_DATA=xxxx # the path to the testing data directory

scripts/run-ldbc-sample2graphar.sh
```

## How to use

Please refer to our [GraphAr Spark Library Documentation](https://graphar.apache.org/docs/libraries/spark/).
