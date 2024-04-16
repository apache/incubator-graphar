# GraphAr Java

This directory contains the code and build system for the GraphAr Java library which powered by [Alibaba-FastFFI](https://github.com/alibaba/fastFFI).

## Dependencies

### Java

- JDK 8 or higher
- Maven3+

### C++

- CMake 3.5 or higher
- LLVM 11

## Install

Only support installing from source currently, but we will support installing from Maven in the future.

Firstly, install llvm-11. `LLVM11_HOME` should point to the home of LLVM 11. In Ubuntu, it is at `/usr/lib/llvm-11`. Basically, the build procedure the following binary:

- `$LLVM11_HOME/bin/clang++`
- `$LLVM11_HOME/bin/ld.lld`
- `$LLVM11_HOME/lib/cmake/llvm`


Tips:
- Use Ubuntu as example:

  ```bash
  $ sudo apt-get install llvm-11 clang-11 lld-11 libclang-11-dev libz-dev -y
  $ export LLVM11_HOME=/usr/lib/llvm-11
  ```

- Or compile from source with this [script](https://github.com/alibaba/fastFFI/blob/main/docker/install-llvm11.sh):
  ```bash
  $ export LLVM11_HOME=/usr/lib/llvm-11
  $ export LLVM_VAR=11.0.0
  $ sudo ./install-llvm11.sh
  ```

Make the graphar-java-library directory as the current working directory:

 ```bash
  $ git clone https://github.com/apache/incubator-graphar.git
  $ cd GraphAr
  $ git submodule update --init
  $ cd java
```

Compile package:

```bash
 $ mvn clean install -DskipTests
```

This will build GraphAr C++ library internally for Java. If you already installed GraphAr C++ library in your system,
you can append this option to skip: `-DbuildGarCPP=OFF`

Then set GraphAr as a dependency in maven project:

```xml
<dependencies>
    <dependency>
      <groupId>org.apache.graphar</groupId>
      <artifactId>gar-java</artifactId>
      <version>0.1.0</version>
    </dependency>
</dependencies>
```

## Generate API document

Building the API document with maven:

```bash
mvn javadoc:javadoc
```

The API document will be generated in the `target/site/apidocs` directory.

## How to use

Please refer to [GraphAr Java Library Documentation](https://graphar.apache.org/GraphAr/user-guide/java-lib.html).