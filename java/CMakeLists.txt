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

# Copyright 2022-2023 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.6)
project(gar-java)

option(BUILD_GAR_CPP "Build GraphAr C++ library with specific version by default, run mvn with `-DbuildGarCPP=OFF` to turn off" ON)
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -std=c++17 -Wall")

# set auto-generated JNI code and handwriting JNI code as source files
file(GLOB SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/target/generated-sources/annotations/*.cc"
                  "${CMAKE_CURRENT_SOURCE_DIR}/target/generated-test-sources/test-annotations/*.cc"
                  "${CMAKE_CURRENT_SOURCE_DIR}/src/main/cpp/ffi/*.cc")
# remove auto-generated JNI code for specific method cause we have handwriting JNI code for it
list(REMOVE_ITEM SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/target/generated-sources/annotations/jni_com_alibaba_graphar_arrow_ArrowTable_Static_cxx_0x58c7409.cc")

set(LIBNAME "gar-jni")

set(JAVA_AWT_LIBRARY NotNeeded)
set(JAVA_AWT_INCLUDE_PATH NotNeeded)
# find JNI related libraries
find_package(JNI REQUIRED)
include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})

# some JNI code depends on arrow directly
find_package(Arrow REQUIRED)

# build the bridge JNI library
add_library(${LIBNAME} SHARED ${SOURCES})
# build GraphAr C++ library or link system installed GraphAr C++ library
if(${BUILD_GAR_CPP})
    include(graphar-cpp)
    build_graphar_cpp()
    target_link_libraries(${LIBNAME} ${CMAKE_JNI_LINKER_FLAGS} gar_shared)
    # include graphar-cpp headers
    target_include_directories(${LIBNAME} SYSTEM BEFORE PRIVATE ${GAR_INCLUDE_DIR})
else()
    find_package(gar REQUIRED)
    target_link_libraries(${LIBNAME} ${CMAKE_JNI_LINKER_FLAGS} gar)
endif()
target_link_libraries(${LIBNAME} ${CMAKE_JNI_LINKER_FLAGS} Arrow::arrow_shared)

set_target_properties(${LIBNAME} PROPERTIES LINKER_LANGUAGE CXX)

# post generated files to target directory
add_custom_command(TARGET ${LIBNAME}
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${LIBNAME}> "${CMAKE_CURRENT_SOURCE_DIR}/target/classes/")

add_custom_command(TARGET ${LIBNAME}
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_SOURCE_DIR}/target/native/bitcode
                   COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_OBJECTS:${LIBNAME}> ${CMAKE_CURRENT_SOURCE_DIR}/target/native/bitcode COMMAND_EXPAND_LISTS)