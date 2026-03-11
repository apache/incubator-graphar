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

cmake_minimum_required(VERSION 3.5)

# Avoid mixing plain and keyword signature of target_link_libraries
if (POLICY CMP0023)
  cmake_policy(SET CMP0023 NEW)
endif()

if(POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif()

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

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
option(GRAPHAR_ENABLE_SANITIZER "Enable address sanitizer (Debug builds only)" ON)
option(GRAPHAR_ENABLE_COVERAGE "Enable coverage instrumentation (Debug builds only)" OFF)

if (USE_STATIC_ARROW)
    set(GRAPHAR_BUILD_STATIC ON)
endif()

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

set(GAR_CXX_STANDARD 17)

if(MSVC)
    # Avoid GCC/Clang-specific flags on MSVC.
    # C++17 is already enforced via CMAKE_CXX_STANDARD/target features.
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror -Wextra -Wno-unused-parameter -Wno-unused-function -Wno-empty-body")
endif()

if (APPLE)
    set(CMAKE_MACOSX_RPATH ON)
elseif(UNIX)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,-rpath,$ORIGIN")
endif ()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -fno-omit-frame-pointer")
if (GRAPHAR_ENABLE_SANITIZER)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=address")
  # Linker flags are required to pull in the ASan runtime.
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG "${CMAKE_EXE_LINKER_FLAGS_DEBUG} -fsanitize=address")
  set(CMAKE_SHARED_LINKER_FLAGS_DEBUG "${CMAKE_SHARED_LINKER_FLAGS_DEBUG} -fsanitize=address")
endif()
if (GRAPHAR_ENABLE_COVERAGE)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} --coverage")
  set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} --coverage")
endif()
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g")

message(STATUS "[graphar] will build in type: ${CMAKE_BUILD_TYPE}")

string(TOUPPER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_UPPER)
if (CMAKE_BUILD_TYPE_UPPER STREQUAL "DEBUG")
  message(STATUS "[graphar] GRAPHAR_ENABLE_SANITIZER: ${GRAPHAR_ENABLE_SANITIZER}")
  message(STATUS "[graphar] GRAPHAR_ENABLE_COVERAGE: ${GRAPHAR_ENABLE_COVERAGE}")
endif()

# ------------------------------------------------------------------------------
# cmake configs
# ------------------------------------------------------------------------------
include(CheckLibraryExists)
include(GNUInstallDirs)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
add_library(${PROJECT_NAME} INTERFACE)
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

# Implementations of lisp "car" and "cdr" functions
macro(GRAPHAR_CAR var)
  set(${var} ${ARGV1})
endmacro()

macro(GRAPHAR_CDR var rest)
  set(${var} ${ARGN})
endmacro()

# Based on MIT-licensed
# https://gist.github.com/cristianadam/ef920342939a89fae3e8a85ca9459b49
function(graphar_create_merged_static_lib output_target)
  set(options)
  set(one_value_args NAME ROOT)
  set(multi_value_args TO_MERGE)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  set(output_lib_path
      ${BUILD_OUTPUT_ROOT_DIRECTORY}${CMAKE_STATIC_LIBRARY_PREFIX}${ARG_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}
  )

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
      if(NOT BUNDLE_TOOL)
        message(FATAL_ERROR "Cannot locate lib.exe to bundle libraries")
      endif()
    endif()
    set(BUNDLE_COMMAND ${BUNDLE_TOOL} /NOLOGO /OUT:${output_lib_path}
                       ${all_library_paths})
  else()
    message(FATAL_ERROR "Unknown bundle scenario!")
  endif()

  add_custom_target(${output_target}_merge ALL
                    ${BUNDLE_COMMAND}
                    DEPENDS ${ARG_ROOT} ${ARG_TO_MERGE}
                    BYPRODUCTS ${output_lib_path}
                    COMMENT "Bundling ${output_lib_path}"
                    VERBATIM)

  message(STATUS "Creating bundled static library target ${output_target} at ${output_lib_path}"
  )

  add_library(${output_target} STATIC IMPORTED)
  set_target_properties(${output_target} PROPERTIES IMPORTED_LOCATION ${output_lib_path})
  add_dependencies(${output_target} ${output_target}_merge)
endfunction()

# ------------------------------------------------------------------------------
# building or find third party library
# ------------------------------------------------------------------------------

include_directories(${CMAKE_CURRENT_BINARY_DIR}/src)
include_directories(src)

if (BUILD_ARROW_FROM_SOURCE)
    # the necessary dependencies for building arrow from source
    find_package(OpenSSL REQUIRED)
    if(OPENSSL_FOUND)
        if(OPENSSL_VERSION LESS "1.1.0")
            message(ERROR "The OpenSSL must be greater than or equal to 1.1.0, current version is  ${OPENSSL_VERSION}")
        endif()
    endif()
    find_package(CURL REQUIRED)

    include(apache-arrow)

    if (ARROW_VERSION_TO_BUILD VERSION_GREATER_EQUAL "23.0.0")
        set(GAR_CXX_STANDARD 20)
    endif()
    build_arrow()
    add_definitions(-DARROW_ORC) # Add macro, otherwise inconsistent in build phase with not from source.
else()
    # check if arrow is installed
    find_package(Arrow QUIET)
    if (NOT ${Arrow_FOUND})
        message(FATAL_ERROR "apache-arrow is required, please install it and retry")
    endif()
    find_package(ArrowDataset QUIET)
    if (NOT ${ArrowDataset_FOUND})
        message(FATAL_ERROR "apache-arrow-dataset is required, please install it and retry")
    endif()
    if (${Arrow_VERSION} VERSION_GREATER_EQUAL "12.0.0")
        # ArrowAcero is available in Arrow 12.0.0 and later
        find_package(ArrowAcero QUIET)
        if (NOT ${ArrowAcero_FOUND})
            message(FATAL_ERROR "apache-arrow-acero is required, please install it and retry")
        endif()
    endif()
    # Check if ORC is enabled.
    if (NOT ${ARROW_ORC})
        message(WARNING "apache-arrow is built without ORC extension, ORC related functionalities will be disabled.")
    else()
        add_definitions(-DARROW_ORC) # Add macro, otherwise inconsistent in build phase on ubuntu.
    endif()

    find_package(Parquet QUIET)
    if (NOT ${Parquet_FOUND})
        message(FATAL_ERROR "parquet is required, please install it and retry")
    endif()

    # Update C++ standard based on detected system Arrow version
    if (${Arrow_VERSION} VERSION_GREATER_EQUAL "23.0.0")
        set(GAR_CXX_STANDARD 20)
        message(STATUS "Arrow ${Arrow_VERSION} detected")
    endif()
endif()

message(STATUS "[graphar] using C++ standard: ${GAR_CXX_STANDARD}")

# ------------------------------------------------------------------------------
# Build core library
# ------------------------------------------------------------------------------
add_subdirectory(src)

# ------------------------------------------------------------------------------
# build examples
# ------------------------------------------------------------------------------
if (BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# ------------------------------------------------------------------------------
# Test targets
# ------------------------------------------------------------------------------
if (BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()

# ------------------------------------------------------------------------------
# Benchmark targets
# ------------------------------------------------------------------------------
if (BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()

# ------------------------------------------------------------------------------
# Install
# ------------------------------------------------------------------------------
install(DIRECTORY ${PROJECT_SOURCE_DIR}/src/graphar
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
