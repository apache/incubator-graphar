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


# Build the Arrow C++ libraries.
function(build_arrow)
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

    # If Arrow needs to be built, the default location will be within the build tree.
    set(GAR_ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix")

    set(GAR_ARROW_STATIC_LIBRARY_DIR "${GAR_ARROW_PREFIX}/lib")

    set(GAR_ARROW_STATIC_LIB_FILENAME
            "${CMAKE_STATIC_LIBRARY_PREFIX}arrow${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GAR_ARROW_STATIC_LIB "${GAR_ARROW_STATIC_LIBRARY_DIR}/${GAR_ARROW_STATIC_LIB_FILENAME}" CACHE INTERNAL "arrow lib")
    set(GAR_PARQUET_STATIC_LIB_FILENAME
	    "${CMAKE_STATIC_LIBRARY_PREFIX}parquet${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GAR_PARQUET_STATIC_LIB "${GAR_ARROW_STATIC_LIBRARY_DIR}/${GAR_PARQUET_STATIC_LIB_FILENAME}" CACHE INTERNAL "parquet lib")
    set(GAR_DATASET_STATIC_LIB_FILENAME
	    "${CMAKE_STATIC_LIBRARY_PREFIX}arrow_dataset${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GAR_DATASET_STATIC_LIB "${GAR_ARROW_STATIC_LIBRARY_DIR}/${GAR_DATASET_STATIC_LIB_FILENAME}" CACHE INTERNAL "arrow dataset lib")
    set(GAR_ARROW_BUNDLED_DEPS_STATIC_LIB_FILENAME
        "${CMAKE_STATIC_LIBRARY_PREFIX}arrow_bundled_dependencies${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
        "${GAR_ARROW_STATIC_LIBRARY_DIR}/${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB_FILENAME}" CACHE INTERNAL "bundled deps lib")

    set(GAR_ARROW_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-build")
    set(GAR_ARROW_CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${GAR_ARROW_PREFIX}"
                             "-DARROW_BUILD_STATIC=ON"
                             "-DARROW_BUILD_SHARED=OFF"
                             "-DARROW_DEPENDENCY_SOURCE=BUNDLED"
                             "-DARROW_DEPENDENCY_USE_SHARED=OFF"
                             "-DCMAKE_INSTALL_LIBDIR=lib"
                             "-Dxsimd_SOURCE=BUNDLED"
                             "-DARROW_PARQUET=ON"
                             "-DARROW_WITH_RE2=OFF"
                             "-DARROW_WITH_UTF8PROC=OFF"
                             "-DARROW_WITH_RE2=OFF"
                             "-DARROW_FILESYSTEM=ON"
                             "-DARROW_CSV=ON"
                             "-DARROW_JSON=ON"
                             "-DARROW_PYTHON=OFF"
                             "-DARROW_BUILD_BENCHMAKRS=OFF"
                             "-DARROW_BUILD_TESTS=OFF"
                             "-DARROW_BUILD_INTEGRATION=OFF"
                             "-DBoost_SOURCE=BUNDLED"
                             "-DARROW_ORC=ON"
                             "-DARROW_COMPUTE=ON"
                             "-DARROW_ACERO=ON"
                             "-DARROW_DATASET=ON"
                             "-DARROW_JEMALLOC=OFF"
                             "-DARROW_WITH_SNAPPY=OFF"
                             "-DARROW_WITH_LZ4=OFF"
                             "-DARROW_WITH_ZSTD=ON"
                             "-DARROW_WITH_ZLIB=OFF"
                             "-DARROW_WITH_BROTLI=OFF"
                             "-DARROW_WITH_BZ2=OFF"
                             "-DARROW_OPENSSL_USE_SHARED=ON"
                             "-DARROW_S3=ON")

    set(GAR_ARROW_INCLUDE_DIR "${GAR_ARROW_PREFIX}/include" CACHE INTERNAL "arrow include directory")
    set(GAR_ARROW_BUILD_BYPRODUCTS "${GAR_ARROW_STATIC_LIB}" "${GAR_PARQUET_STATIC_LIB}" "${GAR_DATASET_STATIC_LIB}")

    find_package(Threads)
    find_package(Arrow QUIET)
    if(DEFINED ENV{GAR_ARROW_SOURCE_URL})
        set(GAR_ARROW_SOURCE_URL "$ENV{GAR_ARROW_SOURCE_URL}")
    else()
        set(ARROW_VERSION_TO_BUILD "15.0.0" CACHE INTERNAL "arrow version")
        if (Arrow_FOUND) # arrow is installed, build the same version as the installed one
            set(ARROW_VERSION_TO_BUILD "${Arrow_VERSION}" CACHE INTERNAL "arrow version")
        endif()
        set(GAR_ARROW_SOURCE_URL "https://www.apache.org/dyn/closer.lua?action=download&filename=arrow/arrow-${ARROW_VERSION_TO_BUILD}/apache-arrow-${ARROW_VERSION_TO_BUILD}.tar.gz")
    endif ()

    include(ExternalProject)
    externalproject_add(arrow_ep
            URL "${GAR_ARROW_SOURCE_URL}"
            SOURCE_SUBDIR cpp
            BINARY_DIR "${GAR_ARROW_BINARY_DIR}"
            CMAKE_ARGS "${GAR_ARROW_CMAKE_ARGS}"
            BUILD_BYPRODUCTS "${GAR_ARROW_BUILD_BYPRODUCTS}")

    set(GAR_ARROW_LIBRARY_TARGET gar_arrow_static)
    set(GAR_PARQUET_LIBRARY_TARGET gar_parquet_static)
    set(GAR_DATASET_LIBRARY_TARGET gar_dataset_static)
    set(GAR_ARROW_BUNDLED_DEPS_TARGET gar_arrow_bundled_dependencies_static)

    file(MAKE_DIRECTORY "${GAR_ARROW_INCLUDE_DIR}")
    add_library(${GAR_ARROW_LIBRARY_TARGET} STATIC IMPORTED)
    add_library(${GAR_PARQUET_LIBRARY_TARGET} STATIC IMPORTED)
    add_library(${GAR_DATASET_LIBRARY_TARGET} STATIC IMPORTED)
    add_library(${GAR_ARROW_BUNDLED_DEPS_TARGET} STATIC IMPORTED)
    set_target_properties(${GAR_ARROW_LIBRARY_TARGET}
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${GAR_ARROW_INCLUDE_DIR}
            IMPORTED_LOCATION ${GAR_ARROW_STATIC_LIB})
    set_target_properties(${GAR_PARQUET_LIBRARY_TARGET}
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${GAR_ARROW_INCLUDE_DIR}
            IMPORTED_LOCATION ${GAR_PARQUET_STATIC_LIB})
    set_target_properties(${GAR_DATASET_LIBRARY_TARGET}
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${GAR_ARROW_INCLUDE_DIR}
            IMPORTED_LOCATION ${GAR_DATASET_STATIC_LIB})
    set_target_properties(${GAR_ARROW_BUNDLED_DEPS_TARGET}
            PROPERTIES IMPORTED_LOCATION ${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB})
    if (ARROW_VERSION_TO_BUILD GREATER_EQUAL "12.0.0")
        set(GAR_ARROW_ACERO_STATIC_LIB_FILENAME
	    "${CMAKE_STATIC_LIBRARY_PREFIX}arrow_acero${CMAKE_STATIC_LIBRARY_SUFFIX}")
        set(GAR_ARROW_ACERO_STATIC_LIB "${GAR_ARROW_STATIC_LIBRARY_DIR}/${GAR_ARROW_ACERO_STATIC_LIB_FILENAME}" CACHE INTERNAL "acero lib")
        set(GAR_ARROW_ACERO_LIBRARY_TARGET gar_acero_static)
        add_library(${GAR_ARROW_ACERO_LIBRARY_TARGET} STATIC IMPORTED)
        set_target_properties(${GAR_ARROW_ACERO_LIBRARY_TARGET}
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${GAR_ARROW_INCLUDE_DIR}
            IMPORTED_LOCATION ${GAR_ARROW_ACERO_STATIC_LIB})
    endif()

    add_dependencies(${GAR_ARROW_LIBRARY_TARGET} arrow_ep)
endfunction()