A PageRank Example
-------------------

This example demonstrates how to compute the PageRank of an input graph using GAR and write back the values as a new property of the graph.

Integrate GAR
^^^^^^^^^^^^^^^^^^

To include GAR C++ library, add the following commands in the CMakeLists.txt:

.. code-block:: cmake

    find_package(gar REQUIRED)
    include_directories(${GAR_INCLUDE_DIRS})
    target_link_libraries(pagerank PRIVATE ${GAR_LIBRARIES})


Build the Project
^^^^^^^^^^^^^^^^^^

.. code-block:: shell

    mkdir build && cd build
    cmake ..
    make


Prepare the input graph
^^^^^^^^^^^^^^^^^^^^^^^^

Copy the ldbc_sample graph from test

.. code-block:: shell

    copy -r ${project_dir}/test/gar-test/ldbc_sample /tmp/


Run the example
^^^^^^^^^^^^^^^^

.. code-block:: shell

    ./pagerank

The output looks like:

.. code-block:: shell

    num_vertices: 903
    iter 0
    iter 1
    iter 2
    iter 3
    iter 4
    iter 5
    iter 6
    iter 7
    iter 8
    iter 9
    Done
