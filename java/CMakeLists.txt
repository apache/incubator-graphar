cmake_minimum_required(VERSION 3.6)
project(gar-java)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -std=c++17 -Wall")

# set auto-generated JNI code and handwriting JNI code as source files
file(GLOB SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/target/generated-sources/annotations/*.cc" "${CMAKE_CURRENT_SOURCE_DIR}/target/generated-test-sources/test-annotations/*.cc"
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

# some JNI code depends on arrow
find_package(Arrow REQUIRED)
# build graphar-cpp in specific version
find_package(gar REQUIRED)
# include(graphar-cpp)
# build_graphar_cpp()

# build the bridge JNI library
add_library(${LIBNAME} SHARED ${SOURCES})
# include graphar-cpp headers
target_include_directories(${LIBNAME} SYSTEM BEFORE PRIVATE ${GAR_INCLUDE_DIR})
# link graphar-cpp and arrow
target_link_libraries(${LIBNAME} ${CMAKE_JNI_LINKER_FLAGS} gar)
target_link_libraries(${LIBNAME} ${CMAKE_JNI_LINKER_FLAGS} Arrow::arrow_static)

set_target_properties(${LIBNAME} PROPERTIES LINKER_LANGUAGE CXX)

# post generated files to target directory
add_custom_command(TARGET ${LIBNAME}
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${LIBNAME}> "${CMAKE_CURRENT_SOURCE_DIR}/target/classes/")

add_custom_command(TARGET ${LIBNAME}
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_SOURCE_DIR}/target/native/bitcode
                   COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_OBJECTS:${LIBNAME}> ${CMAKE_CURRENT_SOURCE_DIR}/target/native/bitcode COMMAND_EXPAND_LISTS)