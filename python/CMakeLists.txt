# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Require CMake 3.15+ (matching scikit-build-core) Use new versions of all
# policies up to CMake 3.27
cmake_minimum_required(VERSION 3.15)

# Scikit-build-core sets these values for you, or you can just hard-code the
# name and version.
project(
  ${SKBUILD_PROJECT_NAME}
  VERSION ${SKBUILD_PROJECT_VERSION}
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../cpp ${CMAKE_BINARY_DIR}/graphar)

# Find the module development requirements (requires FindPython from 3.17 or
# scikit-build-core's built-in backport)
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)
find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(ArrowAcero REQUIRED)
find_package(Parquet REQUIRED)
# Check if ORC is enabled.
if (NOT ${ARROW_ORC})
    message(WARNING "apache-arrow is built without ORC extension, ORC related functionalities will be disabled.")
else()
    add_definitions(-DARROW_ORC) # Add macro, otherwise inconsistent in build phase on ubuntu.
endif()
# Add a library using FindPython's tooling (pybind11 also provides a helper like
# this)
python_add_library(_core MODULE src/bindings/_core_module.cc
                                   src/bindings/cli_binding.cc
                                   src/bindings/graph_info_binding.cc
                                   src/bindings/types_binding.cc WITH_SOABI)
                                   
target_link_libraries(_core PRIVATE pybind11::headers graphar Arrow::arrow_shared
                                    Parquet::parquet_shared
                                    ArrowDataset::arrow_dataset_shared
                                    ArrowAcero::arrow_acero_shared
                                    )
target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../cpp/src)
target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../cpp/thirdparty)

# This is passing in the version as a define just as an example
target_compile_definitions(_core PRIVATE VERSION_INFO=${PROJECT_VERSION})

# The install directory is the output (wheel) directory
# Use platform-appropriate rpath so the Python extension can find the
# packaged libgraphar at runtime inside the wheel.
if(APPLE)
  # macOS uses @loader_path to find libraries relative to the module
  set_target_properties(_core PROPERTIES INSTALL_RPATH "@loader_path")
else()
  # On Linux and other Unix, use $ORIGIN (escaped so CMake preserves the $)
  set_target_properties(_core PROPERTIES INSTALL_RPATH "\$ORIGIN")
endif()

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/client/ DESTINATION client)
install(TARGETS graphar DESTINATION graphar)
install(TARGETS _core DESTINATION graphar)