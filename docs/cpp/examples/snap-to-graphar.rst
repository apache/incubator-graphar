Convert SNAP Datasets to GraphAr Format
=======================================

`SNAP <https://snap.stanford.edu/data/>`_ (Stanford Network Analysis Project) is a general-purpose network analysis and graph mining library. It provides a variety of datasets for research and development. In this section, we will show how to convert the SNAP datasets to GraphAr format, showcasing the process with the `ego-Facebook <https://snap.stanford.edu/data/ego-Facebook.html>`_ graph as a case study. The conversion leverages GraphInfo constructors and the high-level writer functions from the C++ library.


Prepare the SNAP Dataset
------------------------

Before converting, download the ego-Facebook dataset from the SNAP website. The dataset is a text file with each line representing an edge in the graph.

.. code:: bash

  cd /path/to/your/dataset
  wget https://snap.stanford.edu/data/facebook_combined.txt.gz
  gunzip facebook_combined.txt.gz


Convert the SNAP Dataset to GraphAr Format
------------------------------------------

This first step if to first constructs VertexInfo, EdgeInfo, and GraphInfo objects, and then writes them to Yaml files. 
For example, the following code snippet shows how to create and save the vertex info file.

.. code:: C++
  auto version = GAR_NAMESPACE::InfoVersion::Parse("gar/v1").value();

  // meta info
  std::string vertex_label = "node", vertex_prefix = "vertex/node/";

  // create vertex info
  auto vertex_info = GAR_NAMESPACE::CreateVertexInfo(
      vertex_label, VERTEX_CHUNK_SIZE, {}, vertex_prefix, version);

  // save & dump vertex info
  ASSERT(!vertex_info->Dump().has_error());
  ASSERT(vertex_info->Save(save_path + "node.vertex.yml").ok());

  // create and save edge info file ...
  auto edge_info = ...
  ASSERT(!edge_info->Dump().has_error());
  ASSERT(edge_info->Save(save_path + "node_links_node.edge.yml").ok());

  // create and save graph info file ...
  auto graph_info = ...
  ASSERT(!graph_info->Dump().has_error());
  ASSERT(graph_info->Save(save_path + graph_name + ".graph.yml").ok());
 

Next, high-level vertex/edge builders from GraphAr C++ library are used to write the vertex and edge data to payload data files.
The following code snippet shows how to create and save the edge data file.

.. code:: C++
  // construct edges builder
  GAR_NAMESPACE::builder::EdgesBuilder e_builder(edge_info, save_path,
                                                 ADJLIST_TYPE, VERTEX_COUNT);
  // read edge data from file
  std::ifstream file(DATA_PATH);
  std::string line;
  while (std::getline(file, line)) {
    std::istringstream iss(line);
    // skip comments
    if (line[0] == '#') {
      continue;
    }
    int src, dst;
    if (!(iss >> src >> dst)) {
      break;
    }
    GAR_NAMESPACE::builder::Edge e(src, dst);
    ASSERT(e_builder.AddEdge(e).ok());
  }

  // dump & clear
  ASSERT(e_builder.Dump().ok());
  e_builder.Clear();


For more details about this example, please refer to the `source code <https://github.com/alibaba/GraphAr/tree/main/docs/cpp/examples/snap_dataset_to_graphar.cc>`_ .
