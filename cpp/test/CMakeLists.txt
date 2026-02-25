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

find_package(Catch2 3 REQUIRED)

macro(add_graphar_test target)
    set(options)
    set(oneValueArgs)
    set(multiValueArgs SRCS)
    cmake_parse_arguments(add_test "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
    add_executable(${target} ${add_test_SRCS})
    target_compile_features(${target} PRIVATE cxx_std_${GAR_CXX_STANDARD})
    target_include_directories(${target} PRIVATE 
        ${PROJECT_SOURCE_DIR}/thirdparty
        ${PROJECT_SOURCE_DIR}/include 
        $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/thirdparty/Catch2/single_include>
    )
    target_link_libraries(${target} PRIVATE Catch2::Catch2WithMain graphar ${CMAKE_DL_LIBS})
    
    if(BUILD_ARROW_FROM_SOURCE)
        target_include_directories(${target} SYSTEM BEFORE PRIVATE ${GAR_ARROW_INCLUDE_DIR})
        if(APPLE)
            target_link_libraries(${target} PRIVATE -Wl,-force_load gar_arrow_static
                "${GAR_PARQUET_STATIC_LIB}"
                "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}"
                protobuf::libprotobuf
                "-framework CoreFoundation"
                "-framework Security"
                "-framework Network")
        else()
            target_link_libraries(${target} PRIVATE -Wl,-exclude-libs,ALL -Wl,-whole-archive gar_arrow_static
                "${GAR_PARQUET_STATIC_LIB}"
                "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}" -Wl,-no-whole-archive)
        endif()
    else()
        if(APPLE)
            if(USE_STATIC_ARROW)
                target_link_libraries(${target} PRIVATE -Wl,-force_load 
                    Arrow::arrow_static
                    Parquet::parquet_static)
            else()
                target_link_libraries(${target} PRIVATE Arrow::arrow_shared
                    Parquet::parquet_shared)
            endif()
        else()
            if(USE_STATIC_ARROW)
                target_link_libraries(${target} PRIVATE -Wl,-exclude-libs,ALL -Wl,-whole-archive
                    Arrow::arrow_static
                    Parquet::parquet_static -Wl,-no-whole-archive)
            else()
                target_link_libraries(${target} PRIVATE Arrow::arrow_shared
                    Parquet::parquet_shared)
            endif()
        endif()
    endif()
    
    include(CTest)
    include(Catch)
    catch_discover_tests(${target})
endmacro()

add_graphar_test(test_info SRCS test_info.cc)
add_graphar_test(test_arrow_chunk_writer SRCS test_arrow_chunk_writer.cc)
add_graphar_test(test_builder SRCS test_builder.cc)
add_graphar_test(test_chunk_info_reader SRCS test_chunk_info_reader.cc)
add_graphar_test(test_arrow_chunk_reader SRCS test_arrow_chunk_reader.cc)
add_graphar_test(test_graph SRCS test_graph.cc)
add_graphar_test(test_multi_label SRCS test_multi_label.cc)
add_graphar_test(test_multi_property SRCS test_multi_property.cc)
