Contributing to GraphAr
========================

GraphAr has been developed by an active team of software engineers and researchers. Any contributions from the open-source community to improve this project are welcome!

Install Development Dependencies
--------------------------------

GraphAr requires the following C++ packages for development:

- A modern C++ compiler compliant with C++17 standard (g++ >= 7.1 or clang++ >= 5).
- `CMake <https://cmake.org/>`_ (>=2.8)

Build the Source
----------------

You can do an out-of-source build using CMake:

.. code:: shell

    git submodule update --init
    mkdir build
    cd build
    cmake ..
    make -j$(nproc)

The `gar` shared library will be generated under the `build` directory.

Optional: Using a Custom Namespace
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `namespace` that `gar` is defined in is configurable. By default,
it is defined in `namespace GraphArchive`; however this can be toggled by
setting `NAMESPACE` option with cmake:

.. code:: shell

    mkdir build
    cd build
    cmake .. -DNAMESPACE=MyNamespace
    make -j$(nproc)


Run Unit Tests
--------------

GraphAr has included a set of unit tests in the continuous integration process. Test cases can be built from the following commands:

.. code:: shell

    cd build
    cmake .. -DBUILD_TESTS=ON
    make -j$(nproc)

After building test cases, you could run C++ unit tests by:

.. code:: shell

    cd build
    make test

You could only run a specified test case by running the test case binary directly:

.. code:: shell

    cd build
    ./test_info


Documentation
-------------

The documentation is generated using Doxygen and sphinx. Users can build GraphAr's documentation in the :code:`docs/` directory using:

.. code:: bash

    cd build
    make doc

The HTML documentation will be available under `docs/_build/html`:

.. code:: bash

    open _build/html/index.html

The latest version of online documentation can be found at `GraphAr Documentation`_.

The documentation follows the syntax of Doxygen and sphinx markup. If you find anything you can help, please submit pull requests to us. Thanks for your enthusiasm!

Reporting Bugs
--------------

GraphAr is hosted on Github, and use Github issues as the bug tracker. You can `file an issue`_ when you find anything that is expected to work with GraphAr but it doesn't.

Before creating a new bug entry, we recommend you first `search` among existing GraphAr bugs to see if it has already been resolved.

When creating a new bug entry, please provide necessary information of your problem in the description, such as operating system version, GraphAr version, and other system configurations to help us to diagnose the problem.

Code format
^^^^^^^^^^^

GraphAr follows the `Google C++ Style Guide <https://google.github.io/styleguide/cppguide.html>`_. When submitting patches to GraphAr, please format your code with clang-format by the Makefile command `make clformat`, and make sure your code doesn't break the cpplint convention using the CMakefile command `make cpplint`.

Open a pull request
^^^^^^^^^^^^^^^^^^^

When opening issues or submitting pull requests, we'll ask you to prefix the pull request title with the issue number and the kind of patch (`BUGFIX` or `FEATURE`) in brackets, for example, `[BUGFIX-1234] Fix crash in reading arrow table in GraphAr` or `[FEATURE-2345] Support spark writer`.

Git workflow for newcomers
^^^^^^^^^^^^^^^^^^^^^^^^^^

You generally do NOT need to rebase your pull requests unless there are merge
conflicts with the main. When Github complaining that "Can't automatically merge"
on your pull request, you'll be asked to rebase your pull request on top of
the latest main branch, using the following commands:

+ First rebasing to the most recent main:

  .. code:: shell

      git remote add upstream https://github.com/alibaba/GraphAr.git
      git fetch upstream
      git rebase upstream/main

+ Then git may show you some conflicts when it cannot merge, say `conflict.cpp`,
  you need

  - Manually modify the file to resolve the conflicts
  - After resolved, mark it as resolved by:

  .. code:: shell

      git add conflict.cpp

+ Then you can continue rebasing by:

  .. code:: shell

      git rebase --continue

+ Finally push to your fork, then the pull request will be got updated:

  .. code:: shell

      git push --force

.. _file an issue: https://github.com/alibaba/GraphAr/issues/new

.. _GraphAr Documentation: https://alibaba.github.io/GraphAr/
