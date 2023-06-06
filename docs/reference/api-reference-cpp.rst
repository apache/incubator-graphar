C++ API Reference
==================

.. _cpp-api:

.. default-domain:: cpp

Graph Info
-----------

.. doxygenstruct:: GraphArchive::Property
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::PropertyGroup
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::VertexInfo
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::EdgeInfo
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::GraphInfo
    :members:
    :undoc-members:


Readers
---------------------

Chunk Info Reader
~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::VertexPropertyChunkInfoReader
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::AdjListChunkInfoReader
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::AdjListPropertyChunkInfoReader
    :members:
    :undoc-members:

.. doxygenfunction:: GraphArchive::ConstructVertexPropertyChunkInfoReader

.. doxygenfunction:: GraphArchive::ConstructAdjListChunkInfoReader

.. doxygenfunction:: GraphArchive::ConstructAdjListPropertyChunkInfoReader

Arrow Chunk Reader
~~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::VertexPropertyArrowChunkReader
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::AdjListArrowChunkReader
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::AdjListOffsetArrowChunkReader
    :members:
    :undoc-members:

.. doxygenfunction:: GraphArchive::ConstructVertexPropertyArrowChunkReader

.. doxygenfunction:: GraphArchive::ConstructAdjListArrowChunkReader

.. doxygenfunction:: GraphArchive::ConstructAdjListOffsetArrowChunkReader

Vertices Collection
~~~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::Vertex
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::VertexIter
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::VerticesCollection
    :members:
    :undoc-members:

.. doxygenfunction:: GraphArchive::ConstructVerticesCollection

Edges Collection
~~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::Edge
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::EdgesCollection
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::EdgeIter
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::EdgesCollection< AdjListType::ordered_by_source >
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::EdgesCollection< AdjListType::ordered_by_dest >
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::EdgesCollection< AdjListType::unordered_by_source >
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::EdgesCollection< AdjListType::unordered_by_dest >
    :members:
    :undoc-members:

.. doxygenfunction:: GraphArchive::ConstructEdgesCollection

Writer and Builder
---------------------

Chunk Writer
~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::VertexPropertyWriter
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::EdgeChunkWriter
    :members:
    :undoc-members:

Builder
~~~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::builder::Vertex
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::builder::Edge
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::builder::VerticesBuilder
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::builder::EdgesBuilder
    :members:
    :undoc-members:


Types
--------

Id Type
~~~~~~~~~~~~~~~~~~~

.. doxygentypedef:: GraphArchive::IdType

Data Type
~~~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::DataType
    :members:
    :undoc-members:

File Type
~~~~~~~~~~~~~~~~~~~
.. doxygenenum:: GraphArchive::FileType

Adj List Type
~~~~~~~~~~~~~~~~~~~
.. doxygenenum:: GraphArchive::AdjListType

Validate Level
~~~~~~~~~~~~~~~~~~~
.. doxygenenum:: GraphArchive::ValidateLevel


Utilities
---------

Result and Status
~~~~~~~~~~~~~~~~~~~

.. doxygentypedef:: GraphArchive::Result

.. doxygenclass:: GraphArchive::Status
    :members:
    :undoc-members:

FileSystem
~~~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::FileSystem
    :members:
    :undoc-members:

.. doxygenfunction:: GraphArchive::FileSystemFromUriOrPath

Yaml Parser
~~~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::Yaml
    :members:
    :undoc-members:

Info Version
~~~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::InfoVersion
    :members:
    :undoc-members:
