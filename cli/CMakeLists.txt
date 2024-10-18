# Require CMake 3.15+ (matching scikit-build-core) Use new versions of all
# policies up to CMake 3.27
cmake_minimum_required(VERSION 3.15...3.27)

# Scikit-build-core sets these values for you, or you can just hard-code the
# name and version.
project(
  ${SKBUILD_PROJECT_NAME}
  VERSION ${SKBUILD_PROJECT_VERSION}
  LANGUAGES CXX)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/../cpp ${CMAKE_BINARY_DIR}/graphar)

# Find the module development requirements (requires FindPython from 3.17 or
# scikit-build-core's built-in backport)
find_package(Python REQUIRED COMPONENTS Interpreter Development.Module)
find_package(pybind11 CONFIG REQUIRED)

# Add a library using FindPython's tooling (pybind11 also provides a helper like
# this)
python_add_library(_core MODULE src/main.cc WITH_SOABI)
target_link_libraries(_core PRIVATE pybind11::headers graphar)
target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../cpp/src)
target_include_directories(_core PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../cpp/thirdparty)

# This is passing in the version as a define just as an example
target_compile_definitions(_core PRIVATE VERSION_INFO=${PROJECT_VERSION})

# The install directory is the output (wheel) directory
set_target_properties(_core PROPERTIES INSTALL_RPATH "$ORIGIN")
install(TARGETS graphar DESTINATION graphar_cli)
install(TARGETS _core DESTINATION graphar_cli)
