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

cmake_minimum_required(VERSION 3.10)

# Avoid mixing plain and keyword signature of target_link_libraries
if (POLICY CMP0023)
  cmake_policy(SET CMP0023 NEW)
endif()
if(POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif()
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

# ------------------------------------------------------------------------------
# Project Info
# ------------------------------------------------------------------------------
set(GRAPHAR_MAJOR_VERSION 0)
set(GRAPHAR_MINOR_VERSION 13)
set(GRAPHAR_PATCH_VERSION 0)
set(GRAPHAR_VERSION ${GRAPHAR_MAJOR_VERSION}.${GRAPHAR_MINOR_VERSION}.${GRAPHAR_PATCH_VERSION})
project(graphar-cpp LANGUAGES C CXX VERSION ${GRAPHAR_VERSION})

# ------------------------------------------------------------------------------
# cmake options
# ------------------------------------------------------------------------------
option(BUILD_TESTS "Build unit tests" OFF)
option(BUILD_EXAMPLES "Build examples" OFF)
option(BUILD_BENCHMARKS "Build benchmarks" OFF)
option(ENABLE_DOCS "Enable documentation" OFF)
option(BUILD_DOCS_ONLY "Build docs only" OFF)
option(USE_STATIC_ARROW "Link arrow static library" OFF)
option(GRAPHAR_BUILD_STATIC "Build GraphAr as static libraries" OFF)
option(BUILD_ARROW_FROM_SOURCE "Build Arrow from source" OFF)
option(GRAPHAR_ENABLE_ORC "Enable Arrow ORC support (build-from-source)" OFF)
option(GRAPHAR_ENABLE_SANITIZER "Enable address sanitizer (Debug builds only)" ON)

# Consistency Logic
if (USE_STATIC_ARROW OR BUILD_ARROW_FROM_SOURCE)
    set(GRAPHAR_BUILD_STATIC ON)
endif()

# ------------------------------------------------------------------------------
# Build Type & RPATH Configuration
# ------------------------------------------------------------------------------
set(DEFAULT_BUILD_TYPE "Release")
if (NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}' as none was specified.")
  set(CMAKE_BUILD_TYPE "${DEFAULT_BUILD_TYPE}" CACHE STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif ()

# RPATH handling (relocatable install)
set(CMAKE_SKIP_BUILD_RPATH OFF)
set(CMAKE_BUILD_WITH_INSTALL_RPATH OFF)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
    set(CMAKE_INSTALL_RPATH "@loader_path/../lib")
else ()
    set(CMAKE_INSTALL_RPATH "$ORIGIN/../lib")
endif ()

# Compiler Flags
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

if(MSVC)
    # Avoid GCC/Clang-specific flags on MSVC.
    # C++17 is already enforced via CMAKE_CXX_STANDARD/target features.
else()
    # NOTE: Do not add -std=c++17 here (already enforced via target features).
    # Keep warnings consistent for GCC/Clang only.
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
endif()

if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
elseif(UNIX)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,$ORIGIN")
endif ()

# Debug flags (base) - added once.
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -fno-omit-frame-pointer")
if (GRAPHAR_ENABLE_SANITIZER)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
  # Linker flags are required to pull in the ASan runtime.
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fsanitize=address")
  set(CMAKE_SHARED_LINKER_FLAGS_DEBUG "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -fsanitize=address")
endif()
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g")

message(STATUS "[graphar] Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "[graphar] Build Static: ${GRAPHAR_BUILD_STATIC}")
message(STATUS "[graphar] Build Arrow from Source: ${BUILD_ARROW_FROM_SOURCE}")

string(TOUPPER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_UPPER)
if (CMAKE_BUILD_TYPE_UPPER STREQUAL "DEBUG")
  message(STATUS "[graphar] GRAPHAR_ENABLE_SANITIZER: ${GRAPHAR_ENABLE_SANITIZER}")
endif()

# ------------------------------------------------------------------------------
# Dependencies & Configuration
# ------------------------------------------------------------------------------
include(CheckLibraryExists)
include(GNUInstallDirs)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

# Define Interface Library for properties
# FIX 1: Added ${PROJECT_SOURCE_DIR}/src to BUILD_INTERFACE
add_library(${PROJECT_NAME}_interface INTERFACE)
target_compile_features(${PROJECT_NAME}_interface INTERFACE cxx_std_17)
target_include_directories(${PROJECT_NAME}_interface INTERFACE
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
    $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/thirdparty>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

# ------------------------------------------------------------------------------
# Utility Macros & Functions
# ------------------------------------------------------------------------------

# Helper to Link Arrow/Parquet Correctly
function(target_link_arrow_dependencies _TARGET _VISIBILITY)
    if(BUILD_ARROW_FROM_SOURCE)
        # CASE 1: Build from Source -> MUST be Static Linkage
        target_include_directories(${_TARGET} SYSTEM BEFORE PRIVATE ${GAR_ARROW_INCLUDE_DIR})

        set(_DEPS
            ${GAR_ARROW_STATIC_LIB}
            "${GAR_PARQUET_STATIC_LIB}"
            "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}"
        )

        if(APPLE)
            target_link_libraries(${_TARGET} ${_VISIBILITY} -Wl,-force_load ${_DEPS})
        else()
            target_link_libraries(${_TARGET} ${_VISIBILITY}
                -Wl,--exclude-libs,ALL -Wl,--whole-archive ${_DEPS} -Wl,--no-whole-archive)
        endif()

    elseif(USE_STATIC_ARROW)
        # CASE 2: System Install -> Static Linkage requested
        set(_DEPS
            Arrow::arrow_static
            Parquet::parquet_static
            ArrowDataset::arrow_dataset_static
        )
        if(TARGET ArrowAcero::arrow_acero_static)
            list(APPEND _DEPS ArrowAcero::arrow_acero_static)
        endif()

        if(APPLE)
            target_link_libraries(${_TARGET} ${_VISIBILITY} -Wl,-force_load ${_DEPS})
        else()
             target_link_libraries(${_TARGET} ${_VISIBILITY}
                -Wl,--exclude-libs,ALL -Wl,--whole-archive ${_DEPS} -Wl,--no-whole-archive)
        endif()

    else()
        # CASE 3: System Install -> Dynamic Linkage
        set(_DEPS
            Arrow::arrow_shared
            Parquet::parquet_shared
            ArrowDataset::arrow_dataset_shared
        )
        if(TARGET ArrowAcero::arrow_acero_shared)
             list(APPEND _DEPS ArrowAcero::arrow_acero_shared)
        endif()

        target_link_libraries(${_TARGET} ${_VISIBILITY} ${_DEPS})
    endif()
endfunction()

# Macros for List Processing
macro(GRAPHAR_CAR var)
  set(${var} ${ARGV1})
endmacro()

macro(GRAPHAR_CDR var rest)
  set(${var} ${ARGN})
endmacro()

# Function to merge static libraries (for bundling)
function(graphar_create_merged_static_lib output_target)
  set(options)
  set(one_value_args NAME ROOT)
  set(multi_value_args TO_MERGE)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  set(output_lib_path ${BUILD_OUTPUT_ROOT_DIRECTORY}${CMAKE_STATIC_LIBRARY_PREFIX}${ARG_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(all_library_paths $<TARGET_FILE:${ARG_ROOT}>)
  foreach(lib ${ARG_TO_MERGE})
    list(APPEND all_library_paths $<TARGET_FILE:${lib}>)
  endforeach()

  if(APPLE)
    set(BUNDLE_COMMAND "libtool" "-no_warning_for_no_symbols" "-static" "-o"
                                                ${output_lib_path} ${all_library_paths})
    elseif(CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|GNU|Intel)$")
        set(ar_script_path ${CMAKE_BINARY_DIR}/${ARG_NAME}.ar)

        file(WRITE ${ar_script_path}.in "CREATE ${output_lib_path}\n")
        file(APPEND ${ar_script_path}.in "ADDLIB $<TARGET_FILE:${ARG_ROOT}>\n")

        foreach(lib ${ARG_TO_MERGE})
            file(APPEND ${ar_script_path}.in "ADDLIB $<TARGET_FILE:${lib}>\n")
        endforeach()

        file(APPEND ${ar_script_path}.in "SAVE\nEND\n")
        file(GENERATE
                 OUTPUT ${ar_script_path}
                 INPUT ${ar_script_path}.in)

        set(ar_tool ${CMAKE_AR})
        if(CMAKE_INTERPROCEDURAL_OPTIMIZATION)
            set(ar_tool ${CMAKE_CXX_COMPILER_AR})
        endif()

        set(BUNDLE_COMMAND ${ar_tool} -M < ${ar_script_path})
  elseif(MSVC)
    if(CMAKE_LIBTOOL)
      set(BUNDLE_TOOL ${CMAKE_LIBTOOL})
    else()
      find_program(BUNDLE_TOOL lib HINTS "${CMAKE_CXX_COMPILER}/..")
    endif()
        set(BUNDLE_COMMAND ${BUNDLE_TOOL} /NOLOGO /OUT:${output_lib_path} ${all_library_paths})
  else()
     # Linux/Unix AR Script
     set(ar_script_path ${CMAKE_BINARY_DIR}/${ARG_NAME}.ar)
     file(WRITE ${ar_script_path}.in "CREATE ${output_lib_path}\n")
     file(APPEND ${ar_script_path}.in "ADDLIB $<TARGET_FILE:${ARG_ROOT}>\n")
     foreach(lib ${ARG_TO_MERGE})
       file(APPEND ${ar_script_path}.in "ADDLIB $<TARGET_FILE:${lib}>\n")
     endforeach()
     file(APPEND ${ar_script_path}.in "SAVE\nEND\n")
     file(GENERATE OUTPUT ${ar_script_path} INPUT ${ar_script_path}.in)

     set(ar_tool ${CMAKE_AR})
     if(CMAKE_INTERPROCEDURAL_OPTIMIZATION)
       set(ar_tool ${CMAKE_CXX_COMPILER_AR})
     endif()
     set(BUNDLE_COMMAND ${ar_tool} -M < ${ar_script_path})
  endif()

  add_custom_target(${output_target}_merge ALL
                    ${BUNDLE_COMMAND}
                    DEPENDS ${ARG_ROOT} ${ARG_TO_MERGE}
                    BYPRODUCTS ${output_lib_path}
                    COMMENT "Bundling ${output_lib_path}" VERBATIM)

  add_library(${output_target} STATIC IMPORTED)
  set_target_properties(${output_target} PROPERTIES IMPORTED_LOCATION ${output_lib_path})
  add_dependencies(${output_target} ${output_target}_merge)
endfunction()
# ------------------------------------------------------------------------------
# Dependency Acquisition
# ------------------------------------------------------------------------------

if (BUILD_ARROW_FROM_SOURCE)
    find_package(OpenSSL REQUIRED)
    find_package(CURL REQUIRED)
    include(apache-arrow)
    build_arrow()
    if (GRAPHAR_ENABLE_ORC)
        add_definitions(-DARROW_ORC)
    endif()
else()
    # Find System Arrow
    find_package(Arrow QUIET)
    if (NOT Arrow_FOUND)
        message(FATAL_ERROR "apache-arrow is required. Install it or set BUILD_ARROW_FROM_SOURCE=ON")
    endif()
    find_package(ArrowDataset QUIET REQUIRED)
    find_package(Parquet QUIET REQUIRED)

    if (${Arrow_VERSION} VERSION_GREATER_EQUAL "12.0.0")
        find_package(ArrowAcero QUIET REQUIRED)
    endif()

    if (ARROW_ORC)
        add_definitions(-DARROW_ORC)
    endif()
endif()

# ------------------------------------------------------------------------------
# Main Target: GraphAr
# ------------------------------------------------------------------------------
file(GLOB_RECURSE CORE_SRC_FILES
    "src/graphar/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/mini-yaml/yaml/*.cpp"
)

if(GRAPHAR_BUILD_STATIC)
    add_library(graphar STATIC ${CORE_SRC_FILES})
else()
    add_library(graphar SHARED ${CORE_SRC_FILES})
endif()

# Link the interface (includes/definitions)
target_link_libraries(graphar PUBLIC ${PROJECT_NAME}_interface)
target_link_libraries(graphar PRIVATE ${CMAKE_DL_LIBS})

# When building Arrow/Parquet from source, GraphAr's sources (and often its public
# headers) include Arrow/Parquet headers. Ensure the build tree can find them.
# Use BUILD_INTERFACE so we don't leak the build directory into installed targets.
if(BUILD_ARROW_FROM_SOURCE)
    target_include_directories(graphar SYSTEM BEFORE
        PUBLIC $<BUILD_INTERFACE:${GAR_ARROW_INCLUDE_DIR}>
    )
endif()

# Dependency Linking Logic
if(BUILD_ARROW_FROM_SOURCE AND GRAPHAR_BUILD_STATIC)
    set(GAR_BUNDLED_DEPS_STATIC_LIBS
        gar_arrow_static
        gar_parquet_static
        gar_dataset_static
        gar_acero_static
        gar_arrow_bundled_dependencies_static
    )
    GRAPHAR_CAR(_FIRST_LIB ${GAR_BUNDLED_DEPS_STATIC_LIBS})
    GRAPHAR_CDR(_OTHER_LIBS ${GAR_BUNDLED_DEPS_STATIC_LIBS})

    graphar_create_merged_static_lib(graphar_bundled_dependencies
        NAME graphar_bundled_dependencies
        ROOT ${_FIRST_LIB}
        TO_MERGE ${_OTHER_LIBS}
    )

    if(APPLE)
        target_link_libraries(graphar PRIVATE -Wl,-force_load
                graphar_bundled_dependencies
                "-framework CoreFoundation"
                "-framework Security")
    elseif(MSVC)
        # MSVC doesn't support GNU ld-style -Wl flags.
        target_link_libraries(graphar PRIVATE graphar_bundled_dependencies)
    else()
        target_link_libraries(graphar PRIVATE -Wl,--exclude-libs,ALL graphar_bundled_dependencies)
    endif()

    if(OPENSSL_FOUND)
        target_link_libraries(graphar PUBLIC OpenSSL::SSL)
    endif()
    if(CURL_FOUND)
        target_link_libraries(graphar PUBLIC ${CURL_LIBRARIES})
    endif()

    get_target_property(bundled_lib_path graphar_bundled_dependencies IMPORTED_LOCATION)
    install(FILES ${bundled_lib_path} DESTINATION ${CMAKE_INSTALL_LIBDIR})
else()
    if(BUILD_ARROW_FROM_SOURCE OR USE_STATIC_ARROW)
        target_link_arrow_dependencies(graphar PRIVATE)
    else()
        target_link_arrow_dependencies(graphar PUBLIC)
    endif()
endif()

# ------------------------------------------------------------------------------
# Installation
# ------------------------------------------------------------------------------

install(TARGETS graphar ${PROJECT_NAME}_interface
        EXPORT graphar-targets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# FIX 2: Correctly install from src/graphar since that's where headers are
install(DIRECTORY ${PROJECT_SOURCE_DIR}/src/graphar
        DESTINATION include
        FILES_MATCHING PATTERN "*.h"
)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/thirdparty/result DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} FILES_MATCHING PATTERN "*.hpp")

configure_file(graphar-config.in.cmake "${PROJECT_BINARY_DIR}/graphar-config.cmake" @ONLY)
configure_file(graphar-config-version.in.cmake "${PROJECT_BINARY_DIR}/graphar-config-version.cmake" @ONLY)

install(FILES "${PROJECT_BINARY_DIR}/graphar-config.cmake" "${PROJECT_BINARY_DIR}/graphar-config-version.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/graphar)
install(EXPORT graphar-targets FILE graphar-targets.cmake DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/graphar)

# ------------------------------------------------------------------------------
# Examples & Tests
# ------------------------------------------------------------------------------
if (BUILD_EXAMPLES)
    find_package(Boost REQUIRED COMPONENTS graph)
    file(GLOB EXAMPLE_FILES RELATIVE "${PROJECT_SOURCE_DIR}/examples" "${PROJECT_SOURCE_DIR}/examples/*.cc")

    foreach(f ${EXAMPLE_FILES})
        string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
        set(E_NAME ${CMAKE_MATCH_1})
        add_executable(${E_NAME} examples/${E_NAME}.cc)
        target_include_directories(${E_NAME} PRIVATE examples)
        target_include_directories(${E_NAME} SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
        target_link_libraries(${E_NAME} PRIVATE graphar ${Boost_LIBRARIES} ${CMAKE_DL_LIBS})

        # When GraphAr is static, consumers must also link Arrow/Parquet.
        if(GRAPHAR_BUILD_STATIC)
            target_link_arrow_dependencies(${E_NAME} PRIVATE)
        endif()
    endforeach()
endif()

if (BUILD_TESTS)
    find_package(Catch2 3 REQUIRED)
    include(CTest)
    include(Catch)
    macro(add_graphar_test target src_file)
        add_executable(${target} ${src_file})
        target_compile_features(${target} PRIVATE cxx_std_17)
        target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR}/thirdparty)
        target_link_libraries(${target} PRIVATE Catch2::Catch2WithMain graphar ${CMAKE_DL_LIBS})

        # When GraphAr is static, consumers must also link Arrow/Parquet.
        if(GRAPHAR_BUILD_STATIC)
            target_link_arrow_dependencies(${target} PRIVATE)
        endif()
        catch_discover_tests(${target})
    endmacro()

    add_graphar_test(test_info test/test_info.cc)
    add_graphar_test(test_arrow_chunk_writer test/test_arrow_chunk_writer.cc)
    add_graphar_test(test_builder test/test_builder.cc)
    add_graphar_test(test_chunk_info_reader test/test_chunk_info_reader.cc)
    add_graphar_test(test_arrow_chunk_reader test/test_arrow_chunk_reader.cc)
    add_graphar_test(test_graph test/test_graph.cc)
    add_graphar_test(test_multi_label test/test_multi_label.cc)
    add_graphar_test(test_multi_property test/test_multi_property.cc)
endif()

if (BUILD_BENCHMARKS)
    find_package(benchmark REQUIRED)
    macro(add_benchmark target src_file)
        add_executable(${target} ${src_file})
        target_link_libraries(${target} PRIVATE benchmark::benchmark_main graphar)
    endmacro()
    add_benchmark(arrow_chunk_reader_benchmark benchmarks/arrow_chunk_reader_benchmark.cc)
    add_benchmark(label_filter_benchmark benchmarks/label_filter_benchmark.cc)
    add_benchmark(graph_info_benchmark benchmarks/graph_info_benchmark.cc)
endif()

# ------------------------------------------------------------------------------
# Documentation
# ------------------------------------------------------------------------------
if (ENABLE_DOCS OR BUILD_DOCS_ONLY)
    set(PROJECT_DOCUMENT_SOURCE ${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/README.md)
    string(REPLACE ";" " " PROJECT_DOCUMENT_SOURCE "${PROJECT_DOCUMENT_SOURCE}")
    file(DOWNLOAD https://cdn.jsdelivr.net/gh/jothepro/doxygen-awesome-css@2.2.1/doxygen-awesome.min.css ${CMAKE_BINARY_DIR}/doxygen-awesome.css)
    find_package(Doxygen REQUIRED)
    set(DOXYGEN_IN ${PROJECT_SOURCE_DIR}/Doxyfile)
    set(DOXYGEN_OUT ${CMAKE_BINARY_DIR}/Doxyfile.out)
    configure_file(${DOXYGEN_IN} ${DOXYGEN_OUT} @ONLY)
    configure_file(${PROJECT_SOURCE_DIR}/disclaimer_footer.html ${CMAKE_BINARY_DIR}/disclaimer_footer.html COPYONLY)
    add_custom_target(docs COMMAND ${DOXYGEN_EXECUTABLE} ${DOXYGEN_OUT} WORKING_DIRECTORY ${CMAKE_BINARY_DIR} VERBATIM)
    if (BUILD_DOCS_ONLY)
        return()
    endif()
endif()


# ------------------------------------------------------------------------------
# Format code & cpplint
# ------------------------------------------------------------------------------
file(GLOB_RECURSE FILES_NEED_FORMAT "src/graphar/*.h" "src/graphar/*.cc"
                                    "test/*.h" "test/*.cc"
                                    "examples/*.h" "examples/*.cc"
                                    "benchmarks/*.h" "benchmarks/*.cc")
file(GLOB_RECURSE FILES_NEED_LINT "src/graphar/*.h" "src/graphar/*.cc"
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
