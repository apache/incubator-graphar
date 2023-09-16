package com.alibaba.graphar.util;

import com.alibaba.fastffi.CXXHead;
import com.alibaba.fastffi.CXXReference;
import com.alibaba.fastffi.CXXValue;
import com.alibaba.fastffi.FFIGen;
import com.alibaba.fastffi.FFILibrary;
import com.alibaba.fastffi.FFINameAlias;
import com.alibaba.fastffi.FFITypeFactory;
import com.alibaba.graphar.graphinfo.GraphInfo;
import com.alibaba.graphar.graphinfo.PropertyGroup;
import com.alibaba.graphar.readers.arrowchunk.AdjListArrowChunkReader;
import com.alibaba.graphar.readers.arrowchunk.AdjListOffsetArrowChunkReader;
import com.alibaba.graphar.readers.arrowchunk.AdjListPropertyArrowChunkReader;
import com.alibaba.graphar.readers.arrowchunk.VertexPropertyArrowChunkReader;
import com.alibaba.graphar.readers.chunkinfo.AdjListChunkInfoReader;
import com.alibaba.graphar.readers.chunkinfo.AdjListPropertyChunkInfoReader;
import com.alibaba.graphar.readers.chunkinfo.VertexPropertyChunkInfoReader;
import com.alibaba.graphar.stdcxx.StdString;
import com.alibaba.graphar.types.AdjListType;
import com.alibaba.graphar.vertices.VerticesCollection;

import static com.alibaba.graphar.util.CppClassName.GAR_NAMESPACE;
import static com.alibaba.graphar.util.CppHeaderName.GAR_ARROW_CHUNK_READER_H;
import static com.alibaba.graphar.util.CppHeaderName.GAR_CHUNK_INFO_READER_H;
import static com.alibaba.graphar.util.CppHeaderName.GAR_GRAPH_H;

@FFIGen
@CXXHead(GAR_CHUNK_INFO_READER_H)
@CXXHead(GAR_ARROW_CHUNK_READER_H)
@CXXHead(GAR_GRAPH_H)
@FFILibrary(value = GAR_NAMESPACE, namespace = GAR_NAMESPACE)
public interface GrapharStaticFunctions {
  GrapharStaticFunctions INSTANCE = FFITypeFactory.getLibrary(GrapharStaticFunctions.class);

  // chunk info reader

  /**
   * Helper function to Construct VertexPropertyChunkInfoReader.
   *
   * @param graphInfo     The graph info to describe the graph.
   * @param label         label name of the vertex.
   * @param propertyGroup The property group of the vertex.
   */
  @FFINameAlias("ConstructVertexPropertyChunkInfoReader")
  @CXXValue
  Result<VertexPropertyChunkInfoReader> constructVertexPropertyChunkInfoReader(
          @CXXReference GraphInfo graphInfo,
          @CXXReference StdString label,
          @CXXReference PropertyGroup propertyGroup);

  /**
   * Helper function to Construct AdjListPropertyChunkInfoReader.
   *
   * @param graphInfo     The graph info to describe the graph.
   * @param srcLabel      label of source vertex.
   * @param edgeLabel     label of edge.
   * @param dstLabel      label of destination vertex.
   * @param propertyGroup The property group of the edge.
   * @param adjListType   The adj list type for the edges.
   */
  @FFINameAlias("ConstructAdjListPropertyChunkInfoReader")
  @CXXValue
  Result<AdjListPropertyChunkInfoReader> constructAdjListPropertyChunkInfoReader(
          @CXXReference GraphInfo graphInfo,
          @CXXReference StdString srcLabel,
          @CXXReference StdString edgeLabel,
          @CXXReference StdString dstLabel,
          @CXXReference PropertyGroup propertyGroup,
          @CXXValue AdjListType adjListType);

  /**
   * Helper function to Construct AdjListChunkInfoReader.
   *
   * @param graphInfo   The graph info to describe the graph.
   * @param srcLabel    label of source vertex.
   * @param edgeLabel   label of edge.
   * @param dstLabel    label of destination vertex.
   * @param adjListType The adj list type for the edges.
   */
  @FFINameAlias("ConstructAdjListChunkInfoReader")
  @CXXValue
  Result<AdjListChunkInfoReader> constructAdjListChunkInfoReader(
          @CXXReference GraphInfo graphInfo,
          @CXXReference StdString srcLabel,
          @CXXReference StdString edgeLabel,
          @CXXReference StdString dstLabel,
          @CXXValue AdjListType adjListType);

  // arrow chunk reader

  /**
   * Helper function to Construct VertexPropertyArrowChunkReader.
   *
   * @param graphInfo     The graph info to describe the graph.
   * @param label         label of the vertex.
   * @param propertyGroup The property group of the vertex.
   */
  @FFINameAlias("ConstructVertexPropertyArrowChunkReader")
  @CXXValue
  Result<VertexPropertyArrowChunkReader> constructVertexPropertyArrowChunkReader(
          @CXXReference GraphInfo graphInfo,
          @CXXReference StdString label,
          @CXXReference PropertyGroup propertyGroup);

  /**
   * Helper function to Construct AdjListArrowChunkReader.
   *
   * @param graphInfo   The graph info to describe the graph.
   * @param srcLabel    label of source vertex.
   * @param edgeLabel   label of edge.
   * @param dstLabel    label of destination vertex.
   * @param adjListType The adj list type for the edges.
   */
  @FFINameAlias("ConstructAdjListArrowChunkReader")
  @CXXValue
  Result<AdjListArrowChunkReader> constructAdjListArrowChunkReader(
          @CXXReference GraphInfo graphInfo,
          @CXXReference StdString srcLabel,
          @CXXReference StdString edgeLabel,
          @CXXReference StdString dstLabel,
          @CXXValue AdjListType adjListType);

  /**
   * Helper function to Construct AdjListOffsetArrowChunkReader.
   *
   * @param graphInfo   The graph info to describe the graph.
   * @param srcLabel    label of source vertex.
   * @param edgeLabel   label of edge.
   * @param dstLabel    label of destination vertex.
   * @param adjListType The adj list type for the edges.
   */
  @FFINameAlias("ConstructAdjListOffsetArrowChunkReader")
  @CXXValue
  Result<AdjListOffsetArrowChunkReader> constructAdjListOffsetArrowChunkReader(
          @CXXReference GraphInfo graphInfo,
          @CXXReference StdString srcLabel,
          @CXXReference StdString edgeLabel,
          @CXXReference StdString dstLabel,
          @CXXValue AdjListType adjListType);

  /**
   * Helper function to Construct AdjListPropertyArrowChunkReader.
   *
   * @param graphInfo     The graph info to describe the graph.
   * @param srcLabel      label of source vertex.
   * @param edgeLabel     label of edge.
   * @param dstLabel      label of destination vertex.
   * @param propertyGroup The property group of the edge.
   * @param adjListType   The adj list type for the edges.
   */
  @FFINameAlias("ConstructAdjListPropertyArrowChunkReader")
  @CXXValue
  Result<AdjListPropertyArrowChunkReader> constructAdjListPropertyArrowChunkReader(
          @CXXReference GraphInfo graphInfo,
          @CXXReference StdString srcLabel,
          @CXXReference StdString edgeLabel,
          @CXXReference StdString dstLabel,
          @CXXReference PropertyGroup propertyGroup,
          @CXXValue AdjListType adjListType);

  // graph

  /**
   * Construct the collection for vertices with specific label.
   *
   * @param graphInfo The GraphInfo for the graph.
   * @param label     The vertex label.
   * @return The constructed collection or error.
   */
  @FFINameAlias("ConstructVerticesCollection")
  @CXXValue
  Result<VerticesCollection> constructVerticesCollection(
          @CXXReference GraphInfo graphInfo, @CXXReference StdString label);
}
