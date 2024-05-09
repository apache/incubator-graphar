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

cmake_minimum_required(VERSION 2.8)

if(POLICY CMP0026)
  cmake_policy(SET CMP0026 OLD)
endif()

if(POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif()

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

set(GRAPHAR_MAJOR_VERSION 0)
set(GRAPHAR_MINOR_VERSION 11)
set(GRAPHAR_PATCH_VERSION 4)
set(GREAPHAR_VERSION ${GRAPHAR_MAJOR_VERSION}.${GRAPHAR_MINOR_VERSION}.${GRAPHAR_PATCH_VERSION})
project(graphar-cpp LANGUAGES C CXX VERSION ${GREAPHAR_VERSION})

# ------------------------------------------------------------------------------
# cmake options
# ------------------------------------------------------------------------------

option(BUILD_TESTS "Build unit tests" OFF)
option(BUILD_EXAMPLES "Build examples" OFF)
option(BUILD_BENCHMARKS "Build benchmarks" OFF)
option(ENABLE_DOCS "Enable documentation" OFF)
option(BUILD_DOCS_ONLY "Build docs only" OFF)

if (ENABLE_DOCS OR BUILD_DOCS_ONLY)
    set(PROJECT_DOCUMENT_SOURCE ${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/README.md)
    string(REPLACE ";" " " PROJECT_DOCUMENT_SOURCE "${PROJECT_DOCUMENT_SOURCE}")
    file(DOWNLOAD https://cdn.jsdelivr.net/gh/jothepro/doxygen-awesome-css@2.2.1/doxygen-awesome.min.css ${CMAKE_BINARY_DIR}/doxygen-awesome.css)
    find_package(Doxygen REQUIRED)
    set(DOXYGEN_IN ${PROJECT_SOURCE_DIR}/Doxyfile)
    set(DOXYGEN_OUT ${CMAKE_BINARY_DIR}/Doxyfile.out)
    configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
    # Copy disclaimer_footer.html to the build directory
    configure_file(${PROJECT_SOURCE_DIR}/disclaimer_footer.html ${CMAKE_BINARY_DIR}/disclaimer_footer.html COPYONLY)
    add_custom_target(docs
        COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT}
        WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
        COMMENT "Generating API documentation with Doxygen"
        VERBATIM)

    if (BUILD_DOCS_ONLY)
        return()
    endif()
endif()

# ------------------------------------------------------------------------------
# setting default cmake type to Release
# ------------------------------------------------------------------------------
set(DEFAULT_BUILD_TYPE "Release")
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE
      STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
               "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif ()

if(NOT (CMAKE_CXX_COMPILER_LAUNCHER MATCHES "ccache") AND NOT (CMAKE_C_COMPILER_LAUNCHER MATCHES "ccache"))
    find_program(ccache_EXECUTABLE ccache)
    if(ccache_EXECUTABLE)
        set(CMAKE_C_COMPILER_LAUNCHER ${ccache_EXECUTABLE})
        set(CMAKE_CXX_COMPILER_LAUNCHER ${ccache_EXECUTABLE})
        add_custom_target(graphar-ccache-stats
            COMMAND ${ccache_EXECUTABLE} --show-stats
        )
    else()
        add_custom_target(graphar-ccache-stats
            COMMAND echo "ccache not found."
        )
    endif(ccache_EXECUTABLE)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall")

if (APPLE)
  set(CMAKE_MACOSX_RPATH ON)
else ()
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,$ORIGIN")
endif ()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -fno-omit-frame-pointer -fsanitize=address")
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g")

message(STATUS "[graphar] will build in type: ${CMAKE_BUILD_TYPE}")

# ------------------------------------------------------------------------------
# cmake configs
# ------------------------------------------------------------------------------
include(CheckLibraryExists)
include(GNUInstallDirs)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_library(${PROJECT_NAME} INTERFACE)
target_compile_features(${PROJECT_NAME} INTERFACE cxx_std_17)
target_include_directories(
    ${PROJECT_NAME}
    INTERFACE
    $<BUILD_INTERFACE:${${PROJECT_NAME}_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)


# ------------------------------------------------------------------------------
# macro functions
# ------------------------------------------------------------------------------
macro(add_subdirectory_shared directory)
    set(BUILD_SHARED_LIBS_SAVED "${BUILD_SHARED_LIBS}")
    set(BUILD_SHARED_LIBS ON)
    set(CMAKE_BUILD_TYPE_SAVED "${CMAKE_BUILD_TYPE}")
    set(CMAKE_BUILD_TYPE Release)
    add_subdirectory(${directory} ${ARGN})
    set(BUILD_SHARED_LIBS "${BUILD_SHARED_LIBS_SAVED}")
    set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE_SAVED}")
endmacro()

macro(add_subdirectory_static directory)
    set(BUILD_SHARED_LIBS_SAVED "${BUILD_SHARED_LIBS}")
    set(BUILD_SHARED_LIBS OFF)
    set(CMAKE_BUILD_TYPE_SAVED "${CMAKE_BUILD_TYPE}")
    set(CMAKE_BUILD_TYPE Release)
    add_subdirectory(${directory} ${ARGN})
    set(BUILD_SHARED_LIBS "${BUILD_SHARED_LIBS_SAVED}")
    set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE_SAVED}")
endmacro()

macro(find_yaml_cpp)
    set(MESSAGE_QUIET ON)
    set(CMAKE_WARN_DEPRECATED OFF CACHE BOOL "" FORCE)
    set(YAML_CPP_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
    add_subdirectory_static(thirdparty/yaml-cpp EXCLUDE_FROM_ALL)
    unset(MESSAGE_QUIET)
    set(CMAKE_WARN_DEPRECATED ON CACHE BOOL "" FORCE)
endmacro()

macro(find_catch2)
    set(MESSAGE_QUIET ON)
    set(CMAKE_WARN_DEPRECATED OFF CACHE BOOL "" FORCE)
    add_subdirectory_shared(thirdparty/Catch2)
    list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/Catch2/contrib")
    unset(MESSAGE_QUIET)
    set(CMAKE_WARN_DEPRECATED ON CACHE BOOL "" FORCE)
endmacro()

macro(install_graphar_target target)
  # install
  install(TARGETS ${target}
          EXPORT graphar-targets
          ARCHIVE DESTINATION lib
          LIBRARY DESTINATION lib
          RUNTIME DESTINATION bin
          INCLUDES DESTINATION include
  )
endmacro()

# ------------------------------------------------------------------------------
# building or find third party library
# ------------------------------------------------------------------------------
find_package(Threads REQUIRED)
find_package(OpenSSL QUIET)
if (APPLE)
    find_package(curl REQUIRED)
else()
    find_package(CURL REQUIRED)
endif()
if(OPENSSL_FOUND)
    if(OPENSSL_VERSION LESS "1.1.0")
        message(ERROR "The OpenSSL must be greater than or equal to 1.1.0, current version is  ${OPENSSL_VERSION}")
    endif()
endif()

# check if arrow is installed
find_package(Arrow QUIET)
find_package(ArrowDataset QUIET)
find_package(ArrowAcero QUIET)
find_package(Parquet QUIET)

if (NOT Arrow_FOUND OR NOT ArrowDataset_FOUND OR NOT ArrowAcero_FOUND OR NOT Parquet_FOUND)
    message(STATUS "Arrow is not installed, will build from source.")
    set(BUILD_ARROW_FROM_SOURCE ON)
else()
    message(STATUS "-- Found Arrow: ${Arrow_VERSION}")
    set(BUILD_ARROW_FROM_SOURCE OFF)
endif()

if(BUILD_ARROW_FROM_SOURCE)
    include(apache-arrow)
    build_arrow()
endif()


macro(get_target_location var target)
  if(TARGET ${target})
    foreach(prop LOCATION LOCATION_NOCONFIG LOCATION_DEBUG LOCATION_RELEASE)
      get_target_property(${var} ${target} ${prop})
      if(NOT ("${${var}}" STREQUAL "${var}-NOTFOUND"))
        break ()
      endif()
    endforeach()
  endif()
endmacro()

# ------------------------------------------------------------------------------
# generate graphar library
# ------------------------------------------------------------------------------
macro(build_graphar)
    file(GLOB_RECURSE CORE_SRC_FILES "src/*.cc" ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/mini-yaml/yaml/*.cpp)
    add_library(graphar SHARED ${CORE_SRC_FILES})
    install_graphar_target(graphar)
    target_compile_features(graphar PRIVATE cxx_std_17)
    target_include_directories(graphar PUBLIC $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
                                          $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    )
    target_include_directories(graphar PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty)
    if(BUILD_ARROW_FROM_SOURCE)
        target_include_directories(graphar SYSTEM BEFORE PRIVATE ${GAR_ARROW_INCLUDE_DIR})
    endif()
    target_link_libraries(graphar PRIVATE Threads::Threads ${CMAKE_DL_LIBS})

    if(APPLE)
        if(BUILD_ARROW_FROM_SOURCE)
            target_link_libraries(graphar PRIVATE -Wl,-force_load gar_arrow_static
                "${GAR_PARQUET_STATIC_LIB}"
                "${GAR_DATASET_STATIC_LIB}"
                "${GAR_ACERO_STATIC_LIB}"
                "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}")
        else()
            target_link_libraries(graphar PRIVATE -Wl,-force_load Arrow::arrow_shared
                Parquet::parquet_shared
                ArrowDataset::arrow_dataset_shared
                ArrowAcero::arrow_acero_shared)
        endif()
    else()
        if(BUILD_ARROW_FROM_SOURCE)
            target_link_libraries(graphar PRIVATE -Wl,--exclude-libs,ALL -Wl,--whole-archive gar_arrow_static
                "${GAR_PARQUET_STATIC_LIB}"
                "${GAR_DATASET_STATIC_LIB}"
                "${GAR_ARROW_ACERO_STATIC_LIB}"
                "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}" -Wl,--no-whole-archive)
        else()
            target_link_libraries(graphar PRIVATE -Wl,--exclude-libs,ALL -Wl,--whole-archive Arrow::arrow_shared
                Parquet::parquet_shared
                ArrowDataset::arrow_dataset_shared
                ArrowAcero::arrow_acero_shared -Wl,--no-whole-archive)
        endif()
    endif()

    # if OpenSSL library exists, link the OpenSSL library.
    # OpenSSL has to be linked after GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
    if(OPENSSL_FOUND)
        target_link_libraries(graphar PRIVATE OpenSSL::SSL)
    endif()
    if (CURL_FOUND)
        target_link_libraries(graphar PRIVATE ${CURL_LIBRARIES})
    endif()
endmacro()

build_graphar()

# ------------------------------------------------------------------------------
# build example
# ------------------------------------------------------------------------------
if (BUILD_EXAMPLES)
    find_package(Boost REQUIRED COMPONENTS graph)

    file(GLOB EXAMPLE_FILES RELATIVE "${PROJECT_SOURCE_DIR}/examples" "${PROJECT_SOURCE_DIR}/examples/*.cc")
    foreach(f ${EXAMPLE_FILES})
        string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
        set(E_NAME ${CMAKE_MATCH_1})
        message(STATUS "Found example - " ${E_NAME})
        add_executable(${E_NAME} examples/${E_NAME}.cc)
        target_include_directories(${E_NAME} PRIVATE examples
                                                     ${PROJECT_SOURCE_DIR}/include
                                                     ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty
                                                     $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/Catch2/single_include>
        )
        target_include_directories(${E_NAME} SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
        if(BUILD_ARROW_FROM_SOURCE)
            target_include_directories(${E_NAME} SYSTEM BEFORE PRIVATE ${GAR_ARROW_INCLUDE_DIR})
        endif()
        target_link_libraries(${E_NAME} PRIVATE graphar ${Boost_LIBRARIES} Threads::Threads ${CMAKE_DL_LIBS})
        if(APPLE)
            if(BUILD_ARROW_FROM_SOURCE)
                target_link_libraries(${E_NAME} PRIVATE -Wl,-force_load gar_arrow_static
                    "${GAR_PARQUET_STATIC_LIB}"
                    "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}")
            else()
                target_link_libraries(${E_NAME} PRIVATE Arrow::arrow_shared
                    Parquet::parquet_shared)
            endif()
        else()
            if(BUILD_ARROW_FROM_SOURCE)
                target_link_libraries(${E_NAME} PRIVATE -Wl,--exclude-libs,ALL -Wl,--whole-archive gar_arrow_static
                    "${GAR_PARQUET_STATIC_LIB}"
                    "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}" -Wl,--no-whole-archive)
            else()
                target_link_libraries(${E_NAME} PRIVATE Arrow::arrow_shared
                    Parquet::parquet_shared)
            endif()
        endif()

        # if OpenSSL library exists, link the OpenSSL library.
        # OpenSSL has to be linked after GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
        if(OPENSSL_FOUND)
            target_link_libraries(${E_NAME} PRIVATE OpenSSL::SSL)
        endif()
        if (CURL_FOUND)
            target_link_libraries(${E_NAME} PRIVATE CURL::libcurl)
        endif()
    endforeach()
endif()

# ------------------------------------------------------------------------------
# Install
# ------------------------------------------------------------------------------
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/graphar
        DESTINATION include
        FILES_MATCHING
        PATTERN "*.h"
)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/thirdparty/result
        DESTINATION include
        FILES_MATCHING
        PATTERN "*.hpp"
)

configure_file(graphar-config.in.cmake
               "${PROJECT_BINARY_DIR}/graphar-config.cmake" @ONLY
)

configure_file(graphar-config-version.in.cmake
               "${PROJECT_BINARY_DIR}/graphar-config-version.cmake" @ONLY
)

install(FILES "${PROJECT_BINARY_DIR}/graphar-config.cmake"
              "${PROJECT_BINARY_DIR}/graphar-config-version.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/graphar
)

install(EXPORT graphar-targets
        FILE graphar-targets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/graphar
)

# ------------------------------------------------------------------------------
# Test targets
# ------------------------------------------------------------------------------
if (BUILD_TESTS)
    find_catch2()

    macro(add_test target)
        set(options)
        set(oneValueArgs)
        set(multiValueArgs SRCS)
        cmake_parse_arguments(add_test "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
        add_executable(${target} ${add_test_SRCS})
        target_compile_features(${target} PRIVATE cxx_std_17)
        target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR}/thirdparty)
        if(BUILD_ARROW_FROM_SOURCE)
            target_include_directories(${target} SYSTEM BEFORE PRIVATE ${GAR_ARROW_INCLUDE_DIR})
        endif()
        target_link_libraries(${target} PRIVATE Catch2::Catch2 graphar Threads::Threads ${CMAKE_DL_LIBS})
        if(APPLE)
            if(BUILD_ARROW_FROM_SOURCE)
                target_link_libraries(${target} PRIVATE -Wl,-force_load gar_arrow_static
                    "${GAR_PARQUET_STATIC_LIB}"
                    "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}")
            else()
                target_link_libraries(${target} Arrow::arrow_shared
                    Parquet::parquet_shared)
            endif()
        else()
            if(BUILD_ARROW_FROM_SOURCE)
                target_link_libraries(${target} PRIVATE -Wl,--exclude-libs,ALL -Wl,--whole-archive gar_arrow_static
                    "${GAR_PARQUET_STATIC_LIB}"
                    "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}" -Wl,--no-whole-archive)
            else()
                target_link_libraries(${target} PRIVATE Arrow::arrow_shared
                    Parquet::parquet_shared)
            endif()
        endif()
        target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR}/include $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/Catch2/single_include>)
        target_include_directories(${target} SYSTEM BEFORE PRIVATE ${GAR_ARROW_INCLUDE_DIR})
        include(CTest)
        include(Catch)
        catch_discover_tests(${target})

        # if OpenSSL library exists, link the OpenSSL library.
        # OpenSSL has to be linked after GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
        if(OPENSSL_FOUND)
            target_link_libraries(${target} PRIVATE OpenSSL::SSL)
        endif()
        if (CURL_FOUND)
            target_link_libraries(${target} PRIVATE CURL::libcurl)
        endif()
    endmacro()

    add_test(test_info SRCS test/test_info.cc)
    add_test(test_arrow_chunk_writer SRCS test/test_arrow_chunk_writer.cc)
    add_test(test_builder SRCS test/test_builder.cc)
    add_test(test_chunk_info_reader SRCS test/test_chunk_info_reader.cc)
    add_test(test_arrow_chunk_reader SRCS test/test_arrow_chunk_reader.cc)
    add_test(test_graph SRCS test/test_graph.cc)

    # enable_testing()
endif()

if (BUILD_BENCHMARKS)
    find_package(benchmark REQUIRED)

    macro(add_benchmark target)
        set(options)
        set(oneValueArgs)
        set(multiValueArgs SRCS)
        cmake_parse_arguments(add_test "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
        add_executable(${target} ${add_test_SRCS})
        target_compile_features(${target} PRIVATE cxx_std_17)
        target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR}/thirdparty)
        target_link_libraries(${target} PRIVATE benchmark::benchmark_main graphar Threads::Threads ${CMAKE_DL_LIBS})

        # if OpenSSL library exists, link the OpenSSL library.
        # OpenSSL has to be linked after GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
        if(OPENSSL_FOUND)
            target_link_libraries(${target} PRIVATE OpenSSL::SSL)
        endif()
        if (CURL_FOUND)
            target_link_libraries(${target} PRIVATE CURL::libcurl)
        endif()
    endmacro()
    add_benchmark(arrow_chunk_reader_benchmark SRCS benchmarks/arrow_chunk_reader_benchmark.cc)
    add_benchmark(graph_info_benchmark SRCS benchmarks/graph_info_benchmark.cc)
endif()
# ------------------------------------------------------------------------------
# Format code & cpplint
# ------------------------------------------------------------------------------
file(GLOB_RECURSE FILES_NEED_FORMAT "include/graphar/*.h" "src/*.cc"
                                    "test/*.h" "test/*.cc"
                                    "examples/*.h" "examples/*.cc"
                                    "benchmarks/*.h" "benchmarks/*.cc")
file(GLOB_RECURSE FILES_NEED_LINT "include/graphar/*.h" "src/*.cc"
                                  "test/*.h" "test/*.cc"
                                  "examples/*.h" "examples/*.cc"
                                  "benchmarks/*.h" "benchmarks/*.cc")

add_custom_target(graphar-clformat
                  COMMAND clang-format --style=file -i ${FILES_NEED_FORMAT}
                  COMMENT "Running clang-format."
                  VERBATIM)

add_custom_target(graphar-cpplint
        COMMAND ${PROJECT_SOURCE_DIR}/misc/cpplint.py --root=${PROJECT_SOURCE_DIR}/include ${FILES_NEED_LINT}
        COMMENT "Running cpplint check."
        VERBATIM)
