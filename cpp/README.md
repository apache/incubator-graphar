# GraphAr C++

This directory contains the code and build system for the GraphAr C++ library.


## Building GraphAr C++

### System setup

GraphAr C++ uses CMake as a build configuration system. We recommend building
out-of-source. If you are not familiar with this terminology:

- **In-source build**: ``cmake`` is invoked directly from the ``cpp``
  directory. This can be inflexible when you wish to maintain multiple build
  environments (e.g. one for debug builds and another for release builds)
- **Out-of-source build**: ``cmake`` is invoked from another directory,
  creating an isolated build environment that does not interact with any other
  build environment. For example, you could create ``cpp/build-debug`` and
  invoke ``cmake $CMAKE_ARGS ..`` from this directory

Building requires:

- A C++17-enabled compiler. On Linux, gcc 7.1 and higher should be
  sufficient. For MacOS, at least clang 5 is required
- CMake 3.5 or higher
- On Linux and macOS, ``make`` build utilities
- Apache Arrow C++ (>= 12.0.0, requires `arrow-dev`, `arrow-dataset`, `arrow-acero` and `parquet` modules) for Arrow filesystem support. You can refer to [Apache Arrow Installation](https://arrow.apache.org/install/) to install the required modules.

Dependencies for optional features:

- [Doxygen](https://www.doxygen.nl/index.html) (>= 1.8) for generating documentation
- `clang-format-8` for code formatting
- [BGL](https://www.boost.org/doc/libs/1_80_0/libs/graph/doc/index.html) (>= 1.58)
- [Google Benchmark](https://github.com/google/benchmark) (>= 1.6.0) for benchmarking
- [Catch2](https://github.com/catchorg/Catch2) v3 for unit testing

On Ubuntu/Debian, you can install the required packages with:

```bash
sudo apt-get install \
    build-essential \
    cmake \
    libboost-graph-dev \
    doxygen

# Arrow C++ dependencies
wget -c \
    https://apache.jfrog.io/artifactory/arrow/"$(lsb_release --id --short | tr 'A-Z' 'a-z')"/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb \
    -P /tmp/
sudo apt-get install -y /tmp/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
sudo apt-get update
sudo apt-get install -y libarrow-dev libarrow-dataset-dev libarrow-acero-dev libparquet-dev
```

On macOS, you can use [Homebrew](https://brew.sh) to install the required packages:
```bash
brew update && brew bundle --file=cpp/Brewfile
```

> [!NOTE]
> Currently, the Arrow C++ library has [disabled ARROW_ORC](https://github.com/Homebrew/homebrew-core/blob/4588359b7248b07379094de5310ee7ff89afa17e/Formula/a/apache-arrow.rb#L53) in the brew formula, so you need to build and install the Arrow C++ library manually (with `-DARROW_ORC=True`).

### Building

All the instructions below assume that you have cloned the GraphAr git
repository and navigated to the ``cpp`` subdirectory with:

```bash
git clone https://github.com/apache/incubator-graphar.git
cd incubator-graphar/cpp
```

Release build:

```bash
mkdir build-release
cd build-release
cmake ..
make -j8       # if you have 8 CPU cores, otherwise adjust, use -j`nproc` for all cores
```

Debug build with unit tests:

```bash
mkdir build-debug
cd build-debug
cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON ..
make -j8       # if you have 8 CPU cores, otherwise adjust, use -j`nproc` for all cores
```

After building, you can run the unit tests with:

```bash
git clone https://github.com/apache/incubator-graphar-testing.git testing  # download the testing data
GAR_TEST_DATA=${PWD}/testing ctest
```

Build with examples, you should build the project with `BUILD_EXAMPLES` option, then run:

```bash
make -j8       # if you have 8 CPU cores, otherwise adjust, use -j`nproc` for all cores
GAR_TEST_DATA=${PWD}/testing ./bgl_example  # run the BGL example
```

Build with benchmarks, you should build the project with `BUILD_BENCHMARKS` option, then run:

```bash
make -j8       # if you have 8 CPU cores, otherwise adjust, use -j`nproc` for all cores
GAR_TEST_DATA=${PWD}/testing ./graph_info_benchmark  # run the graph info benchmark
```

Extra Build Options:

1. `-DGRAPHAR_BUILD_STATIC=ON`: Build GraphAr as static libraries.
2. `-DUSE_STATIC_ARROW=ON`: Link arrow static library to build GraphAr. If set this option, the option `GRAPHAR_BUILD_STATIC=ON` will be set.

### Building with Arrow from source
In case you want to build GraphAr as single static library including all dependencies, we include a [apache-arrow.cmake](cmake/apache-arrow.cmake) file that allows you to build Arrow and its dependencies from source and link it statically. To do this, you can follow the steps below:

```bash
mkdir build-static
cd build-static
cmake -DGRAPHAR_BUILD_STATIC=ON -DBUILD_ARROW_FROM_SOURCE=ON ..
make -j8    # if you have 8 CPU cores, otherwise adjust, use -j`nproc` for all cores
```

### Install

After the building, you can install the GraphAr C++ library with:

```bash
sudo make install       # run in directory you build, like build-release, build and so on
```

### Generate API document

You should build the project with `ENABLE_DOCS` option. Then run:

```bash
make docs
```

The API document is generated in the directory ``docs_doxygen``.

### Code formatting and linting

To format and lint the code, run:

```bash
cmake ..
make graphar-clformat # format the code
make graphar-cpplint   # lint the code
```

## How to use

Please refer to our [GraphAr C++ API Reference](https://graphar.apache.org/docs/category/c-library).
