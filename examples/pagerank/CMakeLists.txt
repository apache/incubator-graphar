cmake_minimum_required(VERSION 2.8)

# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

project(Pagerank)

find_package(Threads REQUIRED)
find_package(gar REQUIRED)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
include(apache-arrow)
build_arrow()

include_directories(BEFORE SYSTEM ${ARROW_INCLUDE_DIR})

include_directories(${GAR_INCLUDE_DIRS})

add_executable(pagerank pagerank.cc)
target_compile_features(pagerank PRIVATE cxx_std_17)
target_link_libraries(pagerank PRIVATE ${GAR_LIBRARIES})