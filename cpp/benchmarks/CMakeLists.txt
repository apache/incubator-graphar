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

find_package(benchmark REQUIRED)

macro(add_benchmark target)
    set(options)
    set(oneValueArgs)
    set(multiValueArgs SRCS)
    cmake_parse_arguments(add_test "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    add_executable(${target} ${add_test_SRCS})
    target_compile_features(${target} PRIVATE cxx_std_${GAR_CXX_STANDARD})
    target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR}/thirdparty)
    if(APPLE)
        target_link_libraries(${target} PRIVATE benchmark::benchmark_main graphar Arrow::arrow_shared Parquet::parquet_shared ${CMAKE_DL_LIBS})
    else()
        target_link_libraries(${target} PRIVATE benchmark::benchmark_main graphar parquet arrow ${CMAKE_DL_LIBS})
    endif()
    if(BUILD_ARROW_FROM_SOURCE)
        target_include_directories(${target} SYSTEM BEFORE PRIVATE ${GAR_ARROW_INCLUDE_DIR})
        if(APPLE)
            target_link_libraries(${target} PRIVATE -Wl,-force_load ${GAR_ARROW_STATIC_LIB}
                "${GAR_PARQUET_STATIC_LIB}"
                "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}"
                protobuf::libprotobuf
                "-framework CoreFoundation"
                "-framework Security"
                "-framework Network")
        else()
            target_link_libraries(${target} PRIVATE -Wl,-exclude-libs,ALL -Wl,-whole-archive ${GAR_ARROW_STATIC_LIB}
                "${GAR_PARQUET_STATIC_LIB}"
                "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}" -Wl,-no-whole-archive)
        endif()
    endif()
endmacro()

add_benchmark(arrow_chunk_reader_benchmark SRCS arrow_chunk_reader_benchmark.cc)
add_benchmark(label_filter_benchmark SRCS label_filter_benchmark.cc)
add_benchmark(graph_info_benchmark SRCS graph_info_benchmark.cc)
