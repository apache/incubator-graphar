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

# This cmake file is referred and derived from
# https://github.com/apache/arrow/blob/master/matlab/CMakeLists.txt


# Build the GraphAr C++ libraries.
function(build_graphar_cpp)
    set(one_value_args)
    set(multi_value_args)

    cmake_parse_arguments(ARG
            "${options}"
            "${one_value_args}"
            "${multi_value_args}"
            ${ARGN})
    if (ARG_UNPARSED_ARGUMENTS)
        message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
    endif ()

    # If GraphAr needs to be built, the default location will be within the build tree.
    set(GAR_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/graphar_ep-prefix")

    set(GAR_SHARED_LIBRARY_DIR "${GAR_PREFIX}/lib")

    set(GAR_SHARED_LIB_FILENAME
            "${CMAKE_SHARED_LIBRARY_PREFIX}gar${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(GAR_SHARED_LIB "${GAR_SHARED_LIBRARY_DIR}/${GAR_SHARED_LIB_FILENAME}" CACHE INTERNAL "graphar cpp lib")

    set(GAR_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/graphar_ep-build")
    set(GAR_CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${GAR_PREFIX}")

    set(GAR_INCLUDE_DIR "${GAR_PREFIX}/include" CACHE INTERNAL "graphar cpp include directory")
    set(GAR_BUILD_BYPRODUCTS "${GAR_SHARED_LIB}")

    set(GAR_VERSION_TO_BUILD "v0.10.0")

    include(ExternalProject)
    ExternalProject_Add(graphar_ep
            GIT_REPOSITORY https://github.com/alibaba/GraphAr.git
            GIT_TAG ${GAR_VERSION_TO_BUILD}
            GIT_SHALLOW TRUE
            GIT_SUBMODULES ""
            SOURCE_SUBDIR cpp
            BINARY_DIR "${GAR_BINARY_DIR}"
            CMAKE_ARGS "${GAR_CMAKE_ARGS}"
            BUILD_BYPRODUCTS "${GAR_BUILD_BYPRODUCTS}")

    set(GAR_LIBRARY_TARGET gar_shared)

    file(MAKE_DIRECTORY "${GAR_INCLUDE_DIR}")
    add_library(${GAR_LIBRARY_TARGET} SHARED IMPORTED)
    set_target_properties(${GAR_LIBRARY_TARGET}
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${GAR_INCLUDE_DIR}
            IMPORTED_LOCATION ${GAR_SHARED_LIB})

    add_dependencies(${GAR_LIBRARY_TARGET} graphar_ep)
endfunction()
