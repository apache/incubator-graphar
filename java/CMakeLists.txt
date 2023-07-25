cmake_minimum_required(VERSION 3.1)
project(gar-java)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -g -std=c++17 -Wall")

file(GLOB SOURCES "target/generated-sources/annotations/*.cc" "target/generated-test-sources/test-annotations/*.cc")

set(LIBNAME "gar-jni")

set(JAVA_AWT_LIBRARY NotNeeded)
set(JAVA_AWT_INCLUDE_PATH NotNeeded)
find_package(JNI REQUIRED)
include_directories(SYSTEM ${JAVA_INCLUDE_PATH})
include_directories(SYSTEM ${JAVA_INCLUDE_PATH2})

include_directories("src/main/native")
include_directories("src/test/native")

find_package(gar REQUIRED)

add_library(${LIBNAME} SHARED ${SOURCES})
target_link_libraries(${LIBNAME} ${CMAKE_JNI_LINKER_FLAGS} gar)

set_target_properties(${LIBNAME} PROPERTIES LINKER_LANGUAGE CXX)

add_custom_command(TARGET ${LIBNAME}
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${LIBNAME}> "${CMAKE_CURRENT_SOURCE_DIR}/target/classes/")

add_custom_command(TARGET ${LIBNAME}
                   POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_SOURCE_DIR}/target/native/bitcode
                   COMMAND ${CMAKE_COMMAND} -E copy_if_different $<TARGET_OBJECTS:${LIBNAME}> ${CMAKE_CURRENT_SOURCE_DIR}/target/native/bitcode COMMAND_EXPAND_LISTS)
