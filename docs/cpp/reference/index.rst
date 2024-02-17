API Reference
==================

.. _cpp-api:

.. default-domain:: cpp

Graph Info
-----------

.. doxygenclass:: GraphArchive::Property
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::PropertyGroup
    :members:
    :undoc-members:

.. doxygenfunction:: GraphArchive::CreatePropertyGroup

.. doxygenclass:: GraphArchive::AdjacentList
    :members:
    :undoc-members:

.. doxygenfunction:: GraphArchive::CreateAdjacentList

.. doxygenclass:: GraphArchive::VertexInfo
    :members:
    :undoc-members:

.. doxygenfunction:: GraphArchive::CreateVertexInfo

.. doxygenclass:: GraphArchive::EdgeInfo
    :members:
    :undoc-members:

.. doxygenfunction:: GraphArchive::CreateEdgeInfo

.. doxygenclass:: GraphArchive::GraphInfo
    :members:
    :undoc-members:

.. doxygenfunction:: GraphArchive::CreateGraphInfo

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

Edges Collection
~~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::Edge
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::EdgeIter
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::EdgesCollection
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::OBSEdgeCollection
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::OBDEdgesCollection
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::UBSEdgesCollection
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::UBDEdgesCollection
    :members:
    :undoc-members:

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

.. doxygenfunction:: GraphArchive::boolean
.. doxygenfunction:: GraphArchive::int32
.. doxygenfunction:: GraphArchive::int64
.. doxygenfunction:: GraphArchive::float32
.. doxygenfunction:: GraphArchive::float64
.. doxygenfunction:: GraphArchive::string
.. doxygenfunction:: GraphArchive::list

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

Expression
~~~~~~~~~~~~~~~~~~~

.. doxygenclass:: GraphArchive::Expression
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionProperty
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionLiteral
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionNot
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionUnaryOp
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionBinaryOp
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionEqual
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionNotEqual 
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionGreaterThan
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionGreaterEqual
    :members:
    :undoc-members:


.. doxygenclass:: GraphArchive::ExpressionLessThan
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionLessEqual 
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionAnd
    :members:
    :undoc-members:

.. doxygenclass:: GraphArchive::ExpressionOr
    :members:
    :undoc-members:

.. doxygenfunction:: GraphArchive::_Property(const Property&)
.. doxygenfunction:: GraphArchive::_Property(const std::string&)
.. doxygenfunction:: GraphArchive::_Literal
.. doxygenfunction:: GraphArchive::_Not
.. doxygenfunction:: GraphArchive::_Equal
.. doxygenfunction:: GraphArchive::_NotEqual
.. doxygenfunction:: GraphArchive::_GreaterThan
.. doxygenfunction:: GraphArchive::_GreaterEqual
.. doxygenfunction:: GraphArchive::_LessThan
.. doxygenfunction:: GraphArchive::_LessEqual
.. doxygenfunction:: GraphArchive::_And
.. doxygenfunction:: GraphArchive::_Or

