Java Devolopment
================

Introduction
------------

GraphAr Java library based on GraphAr C++ library and an efficient FFI
for Java and C++ called
`FastFFI <https://github.com/alibaba/fastFFI>`__.

Source Code Level
~~~~~~~~~~~~~~~~~

-  Interface

-  Class

-  JNI code

-  C++ source code

Developers only need to write interfaces with annotations. For
annotation's usage, please refer to
`FastFFI <https://github.com/alibaba/fastFFI>`__.

FastFFI will automatically generate ``.java`` files which implement
interfaces, and automatically generate ``.cc`` files which include JNI
code for native methods. Obviously, C++ code has been written.

If llvm4jni is opened, part of JNI code will be transferred to java
class.

Runtime Level
~~~~~~~~~~~~~

Interfaces and classes will be compiled to ``.class`` files.

By writing CMakeLists.txt, all C++ dependents(e.g. JNI code, GraphAr C++
library and other C++ library) will been intergrated into a bridge
dynamic library called gar-jni which can be called by native methods
directly.

Building GraphAr Java
---------------------

Please refer to user guide.

Code Style
----------

We follow `Google Java
style <https://google.github.io/styleguide/javaguide.html>`__. To ensure
CI for checking code style will pass, please ensure check below is
success:

.. code:: bash

   mvn spotless:check

If there are violations, running command below to automatically format:

.. code:: bash

   mvn spotless:apply
