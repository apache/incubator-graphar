cmake_minimum_required(VERSION 2.8)

if(POLICY CMP0048)
  cmake_policy(SET CMP0048 NEW)
endif()

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

set(GAR_MAJOR_VERSION 0)
set(GAR_MINOR_VERSION 1)
set(GAR_PATCH_VERSION 0)
set(GAR_VERSION ${GAR_MAJOR_VERSION}.${GAR_MINOR_VERSION}.${GAR_PATCH_VERSION})
project(graph-archive LANGUAGES C CXX VERSION ${GAR_VERSION})

# ------------------------------------------------------------------------------
# cmake options
# ------------------------------------------------------------------------------

option(NAMESPACE "User specific namespace, default if GraphArchive" OFF)
option(BUILD_TESTS "Build unit test" OFF)
option(BUILD_EXAMPLES "Build examples" OFF)

if (NAMESPACE)
  add_definitions(-DGAR_NAMESPACE=${NAMESPACE})
else()
  add_definitions(-DGAR_NAMESPACE=GraphArchive)
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
        add_custom_target(ccache-stats
            COMMAND ${ccache_EXECUTABLE} --show-stats
        )
    else()
        add_custom_target(ccache-stats
            COMMAND echo "ccache not found."
        )
    endif(ccache_EXECUTABLE)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -Wall")

if (APPLE)
  set(CMAKE_MACOSX_RPATH ON)
else ()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,-rpath,$ORIGIN")
endif ()

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g -fno-omit-frame-pointer -fsanitize=address")
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -g")

message(STATUS "[gar] will build in type: ${CMAKE_BUILD_TYPE}")

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
    add_subdirectory_static(thirdparty/yaml-cpp)
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

macro(install_gar_target target)
  # install
  install(TARGETS ${target}
          EXPORT gar-targets
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
find_yaml_cpp()

include(apache-arrow)
build_arrow()

include_directories(BEFORE SYSTEM ${ARROW_INCLUDE_DIR})

# ------------------------------------------------------------------------------
# generate gar library
# ------------------------------------------------------------------------------
macro(build_gar)
    file(GLOB_RECURSE CORE_SRC_FILES "src/*.cc")
    add_library(gar SHARED ${CORE_SRC_FILES})
    install_gar_target(gar)
    target_compile_features(gar PRIVATE cxx_std_17)
    target_include_directories(
    gar
    PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    $<BUILD_INTERFACE:${${PROJECT_NAME}_SOURCE_DIR}>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/yaml-cpp/include>
    )
    target_link_libraries(gar Threads::Threads ${CMAKE_DL_LIBS})
    if(APPLE)
        target_link_libraries(gar -Wl,-force_load arrow_static
            "${PROJECT_BINARY_DIR}/thirdparty/yaml-cpp/libyaml-cpp.a"
            "${PARQUET_STATIC_LIB}"
            "${ARROW_BUNDLED_DEPS_STATIC_LIB}")
    else()
        target_link_libraries(gar -Wl,--exclude-libs,ALL -Wl,--whole-archive arrow_static
            "${PROJECT_BINARY_DIR}/thirdparty/yaml-cpp/libyaml-cpp.a"
            "${PARQUET_STATIC_LIB}"
            "${ARROW_BUNDLED_DEPS_STATIC_LIB}" -Wl,--no-whole-archive)
    endif()

    # if OpenSSL library exists, link the OpenSSL library.
    # OpenSSL has to be linked after ARROW_BUNDLED_DEPS_STATIC_LIB
    find_package(OpenSSL QUIET)
    if(OPENSSL_FOUND)
        message(STATUS "OpenSSL found with ${OPENSSL_VERSION} version")
        if(OPENSSL_VERSION LESS "1.1.0")
            message(ERROR "The OpenSSL must be greater than or equal to 1.1.0")
        endif()
        target_link_libraries(gar OpenSSL::SSL)
    endif()
endmacro()

build_gar()

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
        target_include_directories(${E_NAME} PRIVATE examples ${PROJECT_SOURCE_DIR}/include $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/Catch2/single_include>)
        include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
        target_link_libraries(${E_NAME} PRIVATE gar ${Boost_LIBRARIES})
    endforeach()
endif()

# ------------------------------------------------------------------------------
# Install
# ------------------------------------------------------------------------------
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/gar
        DESTINATION include
        FILES_MATCHING
        PATTERN "*.h"
)

install(DIRECTORY ${PROJECT_SOURCE_DIR}/include/gar
        DESTINATION include
        FILES_MATCHING
        PATTERN "*.hpp"
)

configure_file(gar-config.in.cmake
               "${PROJECT_BINARY_DIR}/gar-config.cmake" @ONLY
)

configure_file(gar-config-version.in.cmake
               "${PROJECT_BINARY_DIR}/gar-config-version.cmake" @ONLY
)

install(FILES "${PROJECT_BINARY_DIR}/gar-config.cmake"
              "${PROJECT_BINARY_DIR}/gar-config-version.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/gar
)

install(EXPORT gar-targets
        FILE gar-targets.cmake
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/gar
)

# ------------------------------------------------------------------------------
# Test targets
# ------------------------------------------------------------------------------
if (BUILD_TESTS)
    find_catch2()
    add_compile_options(-DBOOST_BIND_GLOBAL_PLACEHOLDERS)
    find_package(Boost REQUIRED COMPONENTS graph)

    macro(add_test target)
        set(options)
        set(oneValueArgs)
        set(multiValueArgs SRCS)
        cmake_parse_arguments(add_test "${options}" "${oneValueArgs}" "${multiValueArgs}" ${ARGN})
        add_executable(${target} ${add_test_SRCS})
        target_compile_features(${target} PRIVATE cxx_std_17)
        target_link_libraries(${target} PRIVATE Catch2::Catch2 gar ${Boost_LIBRARIES})
        target_include_directories(${target} PRIVATE ${PROJECT_SOURCE_DIR}/include $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/Catch2/single_include>)
        include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
        include(CTest)
        include(Catch)
        catch_discover_tests(${target})
    endmacro()

    add_test(test_info SRCS test/test_info.cc)
    add_test(test_arrow_chunk_writer SRCS test/test_arrow_chunk_writer.cc)
    add_test(test_builder SRCS test/test_builder.cc)
    add_test(test_chunk_info_reader SRCS test/test_chunk_info_reader.cc)
    add_test(test_arrow_chunk_reader SRCS test/test_arrow_chunk_reader.cc)
    add_test(test_graph SRCS test/test_graph.cc)

    # enable_testing()
endif()

# ------------------------------------------------------------------------------
# Format code & cpplint
# ------------------------------------------------------------------------------
file(GLOB_RECURSE FILES_NEED_FORMAT "include/gar/*.h" "src/*.cc"
                                    "test/*.h" "test/*.cc")
file(GLOB_RECURSE FILES_NEED_LINT "include/gar/*.h" "src/*.cc"
                                  "test/*.h" "test/*.cc"
                                  )

add_custom_target(clformat
                  COMMAND clang-format --style=file -i ${FILES_NEED_FORMAT}
                  COMMENT "Running clang-format."
                  VERBATIM)

add_custom_target(cpplint
        COMMAND ${PROJECT_SOURCE_DIR}/misc/cpplint.py --root=include ${FILES_NEED_LINT}
        COMMENT "Running cpplint check."
        VERBATIM)

# ------------------------------------------------------------------------------
# build docs
# ------------------------------------------------------------------------------
find_program(doxygen_EXECUTABLE doxygen NO_CMAKE_SYSTEM_PATH)
find_program(sphinx_build_EXECUTABLE sphinx-build NO_CMAKE_SYSTEM_PATH)
if(doxygen_EXECUTABLE AND sphinx_build_EXECUTABLE)
    add_custom_target(doc
        COMMAND ${CMAKE_COMMAND} -E make_directory _build
        COMMAND ${doxygen_EXECUTABLE}
        COMMAND ${sphinx_build_EXECUTABLE} . _build/html
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/docs
        VERBATIM
    )
else()
    if(NOT doxygen_EXECUTABLE)
        message(STATUS "Cannot find the doxygen executable.")
    endif()
    if(NOT sphinx_build_EXECUTABLE)
        message(STATUS "Cannot find the sphinx-build executable.")
    endif()
endif()
