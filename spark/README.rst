GraphAr Spark
=============
This directory contains the code and build system for the GraphAr Spark library.


Building GraphAr Spark
--------------------

System setup
^^^^^^^^^^^^^

GraphAr Spark uses maven as a package build system.

Building requires:

* JDK 8 or higher
* Maven 3.2.0 or higher

Building
^^^^^^^^^

All the instructions below assume that you have cloned the GraphAr git
repository and navigated to the ``spark`` subdirectory:

.. code-block::

    $ git clone https://github.com/alibaba/GraphAr.git
    $ git submodule update --init
    $ cd GraphAr/spark

Build the package:

.. code-block::

    $ mvn clean package -DskipTests

After compilation, the package file graphar-x.x.x-SNAPSHOT-shaded.jar is generated in the directory ``spark/target/``.


Build the package and run the unit tests:

.. code-block::

    $ mvn clean package

Build and run the unit tests:

.. code-block::

    $ mvn clean test

Build and run certain unit test:

.. code-block::

    $ mvn clean test -Dsuites='com.alibaba.graphar.GraphInfoSuite'   # run the GraphInfo test suite
    $ mvn clean test -Dsuites='com.alibaba.graphar.GraphInfoSuite load graph info'  # run the `load graph info` test of test suite


Generate API document
^^^^^^^^^^^^^^^^^^^^

Building the API document with maven:

.. code-block:: shell

    $ mvn scala:doc

The API document is generated in the directory ``spark/target/site/scaladocs``.

How to use
^^^^^^^^^^^

Please refer to our `GraphAr Spark Library Documentation`_.

.. _GraphAr Spark Library Documentation: https://alibaba.github.io/GraphAr/user-guide/spark-lib.html
