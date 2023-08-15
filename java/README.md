# GraphAr Java

## Dependencies
### Java

- JDK 8 or higher
- Maven

### C++
- LLVM 11
- CMake 3.5 or higher
- [GraphAr C++ library](../cpp/README.md)

Tips: 
- To install GraphAr C++ library, you can refer to our [C++ CI](../.github/workflows/ci.yml) on Ubuntu and CentOS;
- To install LLVM 11, you can refer to our [Java CI](../.github/workflows/java.yml) on Ubuntu or compile from source with [this script](https://github.com/alibaba/fastFFI/blob/main/docker/install-llvm11.sh).

## Build, Test and Install
Only support installing from source now, but we will support installing from maven in the future.

```shell
git clone https://github.com/alibaba/GraphAr.git
cd GraphAr
git submodule update --init
# Install GraphAr C++ first ...
cd java
export LLVM11_HOME=<path-to-LLVM11-home>  # In Ubuntu, it is at /usr/lib/llvm-11
mvn clean install
```
