/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "utils/pybind_util.h"

#include "graphar/api/high_level_reader.h"
#include "graphar/api/high_level_writer.h"
#include "graphar/graph_info.h"
#include "graphar/types.h"
#include "graphar/version_parser.h"

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

namespace py = pybind11;

// Changed from PYBIND11_MODULE to a regular function
extern "C" void bind_high_level_api(pybind11::module_& m) {
    // Bind Vertex class
    auto vertex = py::class_<graphar::Vertex, std::shared_ptr<graphar::Vertex>>(m, "Vertex");
    vertex.def("id", &graphar::Vertex::id)
        .def("property", [](const graphar::Vertex& self, const std::string& property) {
            // We need to handle different property types
            // For now, let's support common types used in examples
            try {
                return py::cast(self.property<int64_t>(property).value());
            } catch (...) {
                try {
                    return py::cast(self.property<std::string>(property).value());
                } catch (...) {
                    throw std::runtime_error("Unsupported property type or property not found");
                }
            }
        })
        .def("IsValid", &graphar::Vertex::IsValid);

    // Bind Edge class
    auto edge = py::class_<graphar::Edge, std::shared_ptr<graphar::Edge>>(m, "Edge");
    edge.def("source", &graphar::Edge::source)
        .def("destination", &graphar::Edge::destination)
        .def("property", [](const graphar::Edge& self, const std::string& property) {
            // We need to handle different property types
            // For now, let's support common types used in examples
            try {
                return py::cast(self.property<std::string>(property).value());
            } catch (...) {
                try {
                    return py::cast(self.property<int64_t>(property).value());
                } catch (...) {
                    throw std::runtime_error("Unsupported property type or property not found");
                }
            }
        })
        .def("IsValid", &graphar::Edge::IsValid);

    // Bind VertexIter class
    auto vertex_iter = py::class_<graphar::VertexIter, std::shared_ptr<graphar::VertexIter>>(m, "VertexIter");
    vertex_iter.def("__iter__", [](graphar::VertexIter& it) -> graphar::VertexIter& { return it; })
        .def("__next__", [](graphar::VertexIter& it) {
            // TODO: Implement proper end checking
            auto vertex = *it;
            ++it;
            return vertex;
        })
        .def("id", &graphar::VertexIter::id)
        .def("property", [](graphar::VertexIter& self, const std::string& property) {
            // We need to handle different property types
            // For now, let's support common types used in examples
            try {
                return py::cast(self.property<int64_t>(property).value());
            } catch (...) {
                try {
                    return py::cast(self.property<std::string>(property).value());
                } catch (...) {
                    throw std::runtime_error("Unsupported property type or property not found");
                }
            }
        });

    // Bind VerticesCollection class
    auto vertices_collection = py::class_<graphar::VerticesCollection,
                                     std::shared_ptr<graphar::VerticesCollection>>(m, "VerticesCollection");
    vertices_collection.def("__iter__", [](graphar::VerticesCollection& self) {
            return py::make_iterator(self.begin(), self.end());
        }, py::keep_alive<0, 1>()) // Keep collection alive while iterator is used
        .def("begin", &graphar::VerticesCollection::begin)
        .def("end", &graphar::VerticesCollection::end)
        .def("find", &graphar::VerticesCollection::find)
        .def("size", &graphar::VerticesCollection::size)
        .def_static("Make", [](const std::shared_ptr<graphar::GraphInfo>& graph_info, const std::string& type) {
            auto result = graphar::VerticesCollection::Make(graph_info, type);
            return ThrowOrReturn(result);
        });

    // Bind EdgeIter class
    auto edge_iter = py::class_<graphar::EdgeIter, std::shared_ptr<graphar::EdgeIter>>(m, "EdgeIter");
    edge_iter.def("__iter__", [](graphar::EdgeIter& it) -> graphar::EdgeIter& { return it; })
        .def("__next__", [](graphar::EdgeIter& it) {
            // TODO: Implement proper end checking
            auto edge = *it;
            ++it;
            return edge;
        })
        .def("source", &graphar::EdgeIter::source)
        .def("destination", &graphar::EdgeIter::destination)
        .def("property", [](graphar::EdgeIter& self, const std::string& property) {
            // We need to handle different property types
            // For now, let's support common types used in examples
            try {
                return py::cast(self.property<std::string>(property).value());
            } catch (...) {
                try {
                    return py::cast(self.property<int64_t>(property).value());
                } catch (...) {
                    throw std::runtime_error("Unsupported property type or property not found");
                }
            }
        });

    // Bind EdgesCollection class
    auto edges_collection = py::class_<graphar::EdgesCollection,
                                     std::shared_ptr<graphar::EdgesCollection>>(m, "EdgesCollection");
    edges_collection.def("__iter__", [](graphar::EdgesCollection& self) {
            return py::make_iterator(self.begin(), self.end());
        }, py::keep_alive<0, 1>()) // Keep collection alive while iterator is used
        .def("begin", &graphar::EdgesCollection::begin)
        .def("end", &graphar::EdgesCollection::end)
        .def("size", &graphar::EdgesCollection::size)
        .def("find_src", &graphar::EdgesCollection::find_src)
        .def("find_dst", &graphar::EdgesCollection::find_dst)
        .def_static("Make",  [](const std::shared_ptr<graphar::GraphInfo>& graph_info, 
                              const std::string& src_type,
                              const std::string& edge_type,
                              const std::string& dst_type,
                              graphar::AdjListType adj_list_type) {
            auto result = graphar::EdgesCollection::Make(graph_info, src_type, edge_type, dst_type, adj_list_type);
            return ThrowOrReturn(result);
        });

    // Bind builder::Vertex class
    auto builder_vertex = py::class_<graphar::builder::Vertex, std::shared_ptr<graphar::builder::Vertex>>(m, "BuilderVertex");
    builder_vertex.def(py::init<>())
        .def(py::init<graphar::IdType>())
        .def("GetId", &graphar::builder::Vertex::GetId)
        .def("SetId", &graphar::builder::Vertex::SetId)
        .def("Empty", &graphar::builder::Vertex::Empty)
        .def("AddProperty", [](graphar::builder::Vertex& self, const std::string& name, const py::object& val) {
            // Convert Python object to std::any
            if (py::isinstance<py::int_>(val)) {
                self.AddProperty(name, py::cast<int64_t>(val));
            } else if (py::isinstance<py::str>(val)) {
                self.AddProperty(name, py::cast<std::string>(val));
            } else if (py::isinstance<py::list>(val)) {
                // Handle list properties
                py::list py_list = val.cast<py::list>();
                std::vector<std::string> string_list;
                for (auto item : py_list) {
                    string_list.push_back(py::str(item).cast<std::string>());
                }
                self.AddProperty(graphar::Cardinality::LIST, name, string_list);
            } else {
                throw std::runtime_error("Unsupported property type");
            }
        })
        .def("GetProperty", [](const graphar::builder::Vertex& self, const std::string& property) {
            const auto& prop = self.GetProperty(property);
            // Try to cast to common types
            try {
                return py::cast(std::any_cast<int64_t>(prop));
            } catch (...) {
                try {
                    return py::cast(std::any_cast<std::string>(prop));
                } catch (...) {
                    throw std::runtime_error("Unsupported property type");
                }
            }
        })
        .def("GetProperties", &graphar::builder::Vertex::GetProperties)
        .def("ContainProperty", &graphar::builder::Vertex::ContainProperty);

    //WRITER
    // Bind WriterOptions class
    //TODO add csv_option_builder parquet_option_builder orc_option_builder
    auto writer_options = py::class_<graphar::WriterOptions, std::shared_ptr<graphar::WriterOptions>>(m, "WriterOptions");
    // Bind builder::VerticesBuilder class
    auto vertices_builder = py::class_<graphar::builder::VerticesBuilder, std::shared_ptr<graphar::builder::VerticesBuilder>>(m, "VerticesBuilder");
    vertices_builder.def("Clear", &graphar::builder::VerticesBuilder::Clear)
        .def("SetWriterOptions", &graphar::builder::VerticesBuilder::SetWriterOptions)
        .def("GetWriterOptions", &graphar::builder::VerticesBuilder::GetWriterOptions)
        .def("SetValidateLevel", &graphar::builder::VerticesBuilder::SetValidateLevel)
        .def("GetValidateLevel", &graphar::builder::VerticesBuilder::GetValidateLevel)
        .def("AddVertex", [](graphar::builder::VerticesBuilder& self, graphar::builder::Vertex& v, 
                             graphar::IdType index, const graphar::ValidateLevel& validate_level) {
            return CheckStatus(self.AddVertex(v, index, validate_level));
        }, py::arg("v"), py::arg("index") = -1, py::arg("validate_level") = graphar::ValidateLevel::default_validate)
        .def("GetNum", &graphar::builder::VerticesBuilder::GetNum)
        .def("Dump", [](graphar::builder::VerticesBuilder& self) {
            return CheckStatus(self.Dump());
        });

    // Static factory methods for VerticesBuilder
    vertices_builder.def_static("Make", [](const std::shared_ptr<graphar::VertexInfo>& vertex_info, 
                                           const std::string& prefix,
                                           std::shared_ptr<graphar::WriterOptions> writer_options,
                                           graphar::IdType start_vertex_index,
                                           const graphar::ValidateLevel& validate_level) {
        auto result = graphar::builder::VerticesBuilder::Make(vertex_info, prefix, writer_options, start_vertex_index, validate_level);
        return ThrowOrReturn(result);
    }, py::arg("vertex_info"), py::arg("prefix"), py::arg("writer_options") = nullptr, 
       py::arg("start_vertex_index") = 0, py::arg("validate_level") = graphar::ValidateLevel::no_validate);

    vertices_builder.def_static("Make", [](const std::shared_ptr<graphar::GraphInfo>& graph_info, 
                                           const std::string& type,
                                           std::shared_ptr<graphar::WriterOptions> writer_options,
                                           graphar::IdType start_vertex_index,
                                           const graphar::ValidateLevel& validate_level) {
        auto result = graphar::builder::VerticesBuilder::Make(graph_info, type, writer_options, start_vertex_index, validate_level);
        return ThrowOrReturn(result);
    }, py::arg("graph_info"), py::arg("type"), py::arg("writer_options") = nullptr, 
       py::arg("start_vertex_index") = 0, py::arg("validate_level") = graphar::ValidateLevel::no_validate);
    vertices_builder.def_static("Make", [](const std::shared_ptr<graphar::VertexInfo>& vertex_info, 
                                           const std::string& prefix,
                                           graphar::IdType start_vertex_index) {
        auto result = graphar::builder::VerticesBuilder::Make(vertex_info, prefix, start_vertex_index);
        return ThrowOrReturn(result);
    }, py::arg("vertex_info"), py::arg("prefix"), py::arg("start_vertex_index") = 0);

    // Bind builder::Edge class
    auto builder_edge = py::class_<graphar::builder::Edge, std::shared_ptr<graphar::builder::Edge>>(m, "BuilderEdge");
    builder_edge.def(py::init<graphar::IdType, graphar::IdType>())
        .def("Empty", &graphar::builder::Edge::Empty)
        .def("GetSource", &graphar::builder::Edge::GetSource)
        .def("GetDestination", &graphar::builder::Edge::GetDestination)
        .def("AddProperty", [](graphar::builder::Edge& self, const std::string& name, const py::object& val) {
            // Convert Python object to std::any
            if (py::isinstance<py::int_>(val)) {
                self.AddProperty(name, py::cast<int64_t>(val));
            } else if (py::isinstance<py::str>(val)) {
                self.AddProperty(name, py::cast<std::string>(val));
            } else {
                throw std::runtime_error("Unsupported property type");
            }
        })
        .def("GetProperty", [](const graphar::builder::Edge& self, const std::string& property) {
            const auto& prop = self.GetProperty(property);
            // Try to cast to common types
            try {
                return py::cast(std::any_cast<std::string>(prop));
            } catch (...) {
                try {
                    return py::cast(std::any_cast<int64_t>(prop));
                } catch (...) {
                    throw std::runtime_error("Unsupported property type");
                }
            }
        })
        .def("GetProperties", &graphar::builder::Edge::GetProperties)
        .def("ContainProperty", &graphar::builder::Edge::ContainProperty);

    // Bind builder::EdgesBuilder class
    auto edges_builder = py::class_<graphar::builder::EdgesBuilder, std::shared_ptr<graphar::builder::EdgesBuilder>>(m, "EdgesBuilder");
    edges_builder
        .def("SetValidateLevel", &graphar::builder::EdgesBuilder::SetValidateLevel)
        .def("SetWriterOptions", &graphar::builder::EdgesBuilder::SetWriterOptions)
        .def("GetWriterOptions", &graphar::builder::EdgesBuilder::GetWriterOptions)
        .def("GetValidateLevel", &graphar::builder::EdgesBuilder::GetValidateLevel)
        .def("Clear", &graphar::builder::EdgesBuilder::Clear)
        .def("AddEdge", [](graphar::builder::EdgesBuilder& self, 
                           const graphar::builder::Edge& e,
                           const graphar::ValidateLevel& validate_level) {
            return CheckStatus(self.AddEdge(e, validate_level));
        }, py::arg("e"), py::arg("validate_level") = graphar::ValidateLevel::default_validate)
        .def("GetNum", &graphar::builder::EdgesBuilder::GetNum)
        .def("Dump", [](graphar::builder::EdgesBuilder& self) {
            return CheckStatus(self.Dump());
        });

    // Static factory methods for EdgesBuilder
    edges_builder.def_static("Make", [](const std::shared_ptr<graphar::EdgeInfo>& edge_info,
                                        const std::string& prefix,
                                        graphar::AdjListType adj_list_type,
                                        graphar::IdType num_vertices,
                                        std::shared_ptr<graphar::WriterOptions> writer_options,
                                        const graphar::ValidateLevel& validate_level) {
        auto result = graphar::builder::EdgesBuilder::Make(edge_info, prefix, adj_list_type, num_vertices, writer_options, validate_level);
        return ThrowOrReturn(result);
    }, py::arg("edge_info"), py::arg("prefix"), py::arg("adj_list_type"), py::arg("num_vertices"),
       py::arg("writer_options") = nullptr, py::arg("validate_level") = graphar::ValidateLevel::no_validate);

    edges_builder.def_static("Make", [](const std::shared_ptr<graphar::GraphInfo>& graph_info,
                                        const std::string& src_type,
                                        const std::string& edge_type,
                                        const std::string& dst_type,
                                        const graphar::AdjListType& adj_list_type,
                                        graphar::IdType num_vertices,
                                        std::shared_ptr<graphar::WriterOptions> writer_options,
                                        const graphar::ValidateLevel& validate_level) {
        auto result = graphar::builder::EdgesBuilder::Make(graph_info, src_type, edge_type, dst_type, adj_list_type, num_vertices, writer_options, validate_level);
        return ThrowOrReturn(result);
    }, py::arg("graph_info"), py::arg("src_type"), py::arg("edge_type"), py::arg("dst_type"),
       py::arg("adj_list_type"), py::arg("num_vertices"), py::arg("writer_options") = nullptr,
       py::arg("validate_level") = graphar::ValidateLevel::no_validate);
}  // namespace graphar