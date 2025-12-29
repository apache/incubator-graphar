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

# The Python wheel build (e.g. via cibuildwheel) may run from an isolated copy of
# the `python/` directory, where `../cpp` doesn't exist. In CI we bundle `cpp/`
# into the project directory as `_bundled_cpp/`.
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/_bundled_cpp/CMakeLists.txt")
  set(GRAPHAR_CPP_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/_bundled_cpp")
elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/../cpp/CMakeLists.txt")
  set(GRAPHAR_CPP_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../cpp")
else()
  message(FATAL_ERROR "GraphAr C++ sources not found. Expected either '_bundled_cpp/' or '../cpp/'.")
endif()

# Verify that the expected subdirectories exist before adding the C++ sources.
if(NOT EXISTS "${GRAPHAR_CPP_SOURCE_DIR}/src")
  message(FATAL_ERROR "GraphAr C++ sources are missing the 'src' directory under '${GRAPHAR_CPP_SOURCE_DIR}'.")
endif()
if(NOT EXISTS "${GRAPHAR_CPP_SOURCE_DIR}/thirdparty")
  message(FATAL_ERROR "GraphAr C++ sources are missing the 'thirdparty' directory under '${GRAPHAR_CPP_SOURCE_DIR}'.")
endif()

add_subdirectory(${GRAPHAR_CPP_SOURCE_DIR} ${CMAKE_BINARY_DIR}/graphar)

# Find the module development requirements (requires FindPython from 3.17 or
# scikit-build-core's built-in backport)
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)
find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)
find_package(ArrowAcero REQUIRED)
find_package(Parquet REQUIRED)

if(APPLE)
  # cibuildwheel uses delocate to bundle external dylibs into the wheel.
  # delocate needs to be able to resolve @rpath dependencies (e.g.
  # @rpath/libarrow.1300.dylib). We add the imported Arrow/Parquet library
  # directories to LC_RPATH so delocate can find and copy them.
  set(_graphar_macos_rpaths "")

  foreach(_tgt IN ITEMS Arrow::arrow_shared ArrowDataset::arrow_dataset_shared ArrowAcero::arrow_acero_shared Parquet::parquet_shared)
    if(TARGET ${_tgt})
      get_target_property(_loc ${_tgt} IMPORTED_LOCATION_RELEASE)
      if(NOT _loc)
        get_target_property(_loc ${_tgt} IMPORTED_LOCATION)
      endif()
      if(_loc)
        get_filename_component(_libdir "${_loc}" DIRECTORY)
        list(APPEND _graphar_macos_rpaths "${_libdir}")
      endif()
    endif()
  endforeach()

  list(REMOVE_DUPLICATES _graphar_macos_rpaths)
endif()
# Check if ORC is enabled.
if (NOT ${ARROW_ORC})
    message(WARNING "apache-arrow is built without ORC extension, ORC related functionalities will be disabled.")
else()
    add_definitions(-DARROW_ORC) # Add macro, otherwise inconsistent in build phase on ubuntu.
endif()
# Add a library using FindPython's tooling (pybind11 also provides a helper like
# this)
python_add_library(_core MODULE src/bindings/types_binding.cc
                                   src/bindings/cli_binding.cc
                                   src/bindings/graph_info_binding.cc
                                   src/bindings/high_level_binding.cc
                                   src/bindings/_core_module.cc WITH_SOABI)
                                   
target_link_libraries(_core PRIVATE pybind11::headers graphar Arrow::arrow_shared
                                    Parquet::parquet_shared
                                    ArrowDataset::arrow_dataset_shared
                                    ArrowAcero::arrow_acero_shared
                                    )
target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(_core PRIVATE ${GRAPHAR_CPP_SOURCE_DIR}/src)
target_include_directories(_core PRIVATE ${GRAPHAR_CPP_SOURCE_DIR}/thirdparty)

# This is passing in the version as a define just as an example
target_compile_definitions(_core PRIVATE VERSION_INFO=${PROJECT_VERSION})

# The install directory is the output (wheel) directory.
# Use platform-appropriate rpath so the Python extension can find the
# packaged libgraphar at runtime inside the wheel.
if(APPLE)
  # Keep @loader_path for runtime inside the wheel, and add Arrow/Parquet
  # library dirs so delocate can resolve @rpath dependencies during repair.
  set(_core_rpath "@loader_path")
  if(_graphar_macos_rpaths)
    list(JOIN _graphar_macos_rpaths ";" _graphar_macos_rpaths_joined)
    set(_core_rpath "${_core_rpath};${_graphar_macos_rpaths_joined}")
  endif()
  set_target_properties(_core PROPERTIES BUILD_WITH_INSTALL_RPATH ON)
  set_target_properties(_core PROPERTIES INSTALL_RPATH "${_core_rpath}")

  if(TARGET graphar)
    set_target_properties(graphar PROPERTIES BUILD_WITH_INSTALL_RPATH ON)
    if(_graphar_macos_rpaths)
      set_target_properties(graphar PROPERTIES INSTALL_RPATH "${_graphar_macos_rpaths_joined}")
    endif()
  endif()
else()
  # On Linux and other Unix, use $ORIGIN (escaped so CMake preserves the $)
  set_target_properties(_core PROPERTIES INSTALL_RPATH "\$ORIGIN")
endif()

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/src/cli/ DESTINATION cli)
install(TARGETS graphar DESTINATION graphar)
install(TARGETS _core DESTINATION graphar)