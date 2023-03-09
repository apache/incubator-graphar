GraphAr C++
============
This directory contains the code and build system for the GraphAr C++ library.


Building GraphAr C++
--------------------

System setup
^^^^^^^^^^^^

GraphAr C++ uses CMake as a build configuration system. We recommend building
out-of-source. If you are not familiar with this terminology:

* **In-source build**: ``cmake`` is invoked directly from the ``cpp``
  directory. This can be inflexible when you wish to maintain multiple build
  environments (e.g. one for debug builds and another for release builds)
* **Out-of-source build**: ``cmake`` is invoked from another directory,
  creating an isolated build environment that does not interact with any other
  build environment. For example, you could create ``cpp/build-debug`` and
  invoke ``cmake $CMAKE_ARGS ..`` from this directory

Building requires:

* A C++17-enabled compiler. On Linux, gcc 7.1 and higher should be
  sufficient. For MacOS, at least clang 5 is required
* CMake 3.5 or higher
* On Linux and macOS, ``make`` build utilities

Dependencies for optional features:

* `Doxygen <https://www.doxygen.nl/index.html>`_ (>= 1.8) for generating documentation

Extra dependencies are required by examples:

* `BGL <https://www.boost.org/doc/libs/1_80_0/libs/graph/doc/index.html>`_ (>= 1.58)


Building
^^^^^^^^^

All the instructions below assume that you have cloned the GraphAr git
repository and navigated to the ``cpp`` subdirectory:

.. code-block::

    $ git clone https://github.com/alibaba/GraphAr.git
    $ git submodule update --init
    $ cd GraphAr/cpp

Release build:

.. code-block::

    $ mkdir build-release
    $ cd build-release
    $ cmake ..
    $ make -j8       # if you have 8 CPU cores, otherwise adjust

Build with a custom namespace:

The :code:`namespace` is configurable. By default,
it is defined in :code:`namespace GraphArchive`; however this can be toggled by
setting :code:`NAMESPACE` option with cmake:

.. code:: shell

    $ mkdir build
    $ cd build
    $ cmake .. -DNAMESPACE=MyNamespace
    $ make -j8       # if you have 8 CPU cores, otherwise adjust

Debug build with unit tests:

.. code-block:: shell

    $ export GAR_TEST_DATA=$PWD/../testing/
    $ mkdir build-debug
    $ cd build-debug
    $ cmake -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON ..
    $ make -j8       # if you have 8 CPU cores, otherwise adjust
    $ make test      # to run the tests

Build with examples:

.. code-block:: shell

    $ export GAR_TEST_DATA=$PWD/../testing/
    $ mkdir build-examples
    $ cd build-examples
    $ cmake -DBUILD_EXAMPLES=ON ..
    $ make -j8       # if you have 8 CPU cores, otherwise adjust

Install
^^^^^^^^^

After the building, you can install the GraphAr C++ library with:

.. code-block:: shell

    $ sudo make install

Generate API document
^^^^^^^^^^^^^^^^^^^^^

Building the API document with Doxgen:

.. code-block:: shell

    $ pushd apidoc
    $ doxgen
    $ popd

The API document is generated in the directory ``cpp/apidoc/html``.
