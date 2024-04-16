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
- curl-devel with SSL (Linux) or curl (macOS), for s3 filesystem support
- Apache Arrow C++ (>= 12.0.0, requires `arrow-dev`, `arrow-dataset`, `arrow-acero` and `parquet` modules) for Arrow filesystem support and can use `BUILD_ARROW_FROM_SOURCE` option to build with GraphAr automatically. You can refer to [Apache Arrow Installation](https://arrow.apache.org/install/) to install Arrow directly too.

Dependencies for optional features:

- [Doxygen](https://www.doxygen.nl/index.html) (>= 1.8) for generating documentation

Extra dependencies are required by examples:

- [BGL](https://www.boost.org/doc/libs/1_80_0/libs/graph/doc/index.html) (>= 1.58)

### Building

All the instructions below assume that you have cloned the GraphAr git
repository and navigated to the ``cpp`` subdirectory:

```bash
    $ git clone https://github.com/apache/incubator-graphar.git
    $ cd GraphAr
    $ git submodule update --init
    $ cd cpp
```

Release build:

```bash
    $ mkdir build-release
    $ cd build-release
    $ cmake ..
    $ make -j8       # if you have 8 CPU cores, otherwise adjust, use -j`nproc` for all cores
```

Build the Apache Arrow dependency from source:

By default, GraphAr try to find Apache Arrow in the system. This can be configured to build Arrow dependency automatically from source:

```bash
    $ mkdir build
    $ cd build
    $ cmake -DBUILD_ARROW_FROM_SOURCE=ON ..
    $ make -j8
```

Debug build with unit tests:

```bash
    $ export GAR_TEST_DATA=$PWD/../testing/
    $ mkdir build-debug
    $ cd build-debug
    $ cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON ..
    $ make -j8       # if you have 8 CPU cores, otherwise adjust, use -j`nproc` for all cores
    $ make test      # to run the tests
```

Build with examples:

```bash
    $ export GAR_TEST_DATA=$PWD/../testing/
    $ mkdir build-examples
    $ cd build-examples
    $ cmake -DBUILD_EXAMPLES=ON ..
    $ make -j8       # if you have 8 CPU cores, otherwise adjust, use -j`nproc` for all cores
    $ ./bgl_example  # run the BGL example
```

Build with benchmarks:

```bash
    $ export GAR_TEST_DATA=$PWD/../testing/
    $ mkdir build-benchmarks
    $ cd build-benchmarks
    $ cmake -DBUILD_BENCHMARKS=ON ..
    $ make -j8       # if you have 8 CPU cores, otherwise adjust, use -j`nproc` for all cores
    $ ./graph_info_benchmark  # run the graph info benchmark
```

### Install

After the building, you can install the GraphAr C++ library with:

```bash
    $ sudo make install       # run in directory you build, like build-release, build and so on
```

### Generate API document

You should build the project with `ENABLE_DOCS` option. Then run:

```bash
   make docs
```

The API document is generated in the directory ``docs_doxygen``.

## How to use

Please refer to our [GraphAr C++ API Reference](https://graphar.apache.org/docs/libraries/cpp).

