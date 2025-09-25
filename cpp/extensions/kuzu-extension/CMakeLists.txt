if (WIN32 OR BUILD_STATIC_EXTENSION)
    set(GraphAr_USE_STATIC_LIBS   ON)
    set(Arrow_USE_STATIC_LIBS     ON)
else()
    set(GraphAr_USE_STATIC_LIBS   OFF)
    set(Arrow_USE_STATIC_LIBS     OFF)
endif()

find_package(graphar REQUIRED)
find_package(Arrow REQUIRED)

message(STATUS "Found GraphAr  : ${graphar_VERSION}")
message(STATUS "Found Arrow   : ${Arrow_VERSION}")

include_directories(
    ${PROJECT_SOURCE_DIR}/src/include
    ${CMAKE_BINARY_DIR}/src/include
    ${PROJECT_SOURCE_DIR}/extension/graphar/src/include
    src/include
)

add_subdirectory(src/function)
add_subdirectory(src/main)

build_extension_lib(${BUILD_STATIC_EXTENSION} "graphar")

target_link_libraries(
    kuzu_${EXTENSION_LIB_NAME}_extension
    PUBLIC
      graphar
    #   Arrow::arrow_shared
)

if (APPLE AND NOT BUILD_STATIC_EXTENSION)
    set_apple_dynamic_lookup(kuzu_${EXTENSION_LIB_NAME}_extension)
endif()
