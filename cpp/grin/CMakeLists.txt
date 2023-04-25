# build GraphAr-grin

find_package(Protobuf REQUIRED)

file(GLOB_RECURSE GAR_GRIN_SRC_FILES "src/*.cc")

message(${GAR_GRIN_SRC_FILES})

add_library(gar-grin SHARED ${GAR_GRIN_SRC_FILES})

target_compile_features(gar-grin PUBLIC cxx_std_17)

target_include_directories(gar-grin SYSTEM BEFORE PUBLIC include ${Protobuf_INCLUDE_DIRS})

target_link_libraries(gar-grin PUBLIC gar ${Protobuf_LIBRARIES})

# ------------------------------------------------------------------------------
# build tests
# ------------------------------------------------------------------------------
file(GLOB TEST_FILES RELATIVE "${PROJECT_SOURCE_DIR}/grin/test" "${PROJECT_SOURCE_DIR}/grin/test/*.cc")

foreach(f ${TEST_FILES})
    string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
    set(E_NAME ${CMAKE_MATCH_1})
    message(STATUS "Found test - " ${E_NAME})
    add_executable(${E_NAME} test/${E_NAME}.cc)
    target_include_directories(${E_NAME} SYSTEM BEFORE PRIVATE ${PROJECT_SOURCE_DIR}/include ${Protobuf_INCLUDE_DIRS})
    target_link_libraries(${E_NAME} PRIVATE gar-grin Threads::Threads ${CMAKE_DL_LIBS} ${Protobuf_LIBRARIES})

    # if OpenSSL library exists, link the OpenSSL library.
    # OpenSSL has to be linked after GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
    if(OPENSSL_FOUND)
        target_link_libraries(${E_NAME} PRIVATE OpenSSL::SSL)
    endif()
endforeach()

# ------------------------------------------------------------------------------
# build examples
# ------------------------------------------------------------------------------
file(GLOB EXAMPLE_FILES RELATIVE "${PROJECT_SOURCE_DIR}/grin/example" "${PROJECT_SOURCE_DIR}/grin/example/*.cc")

foreach(f ${EXAMPLE_FILES})
    string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
    set(E_NAME ${CMAKE_MATCH_1})
    message(STATUS "Found example - " ${E_NAME})
    add_executable(${E_NAME} example/${E_NAME}.cc)
    target_include_directories(${E_NAME} SYSTEM BEFORE PRIVATE ${PROJECT_SOURCE_DIR}/include ${Protobuf_INCLUDE_DIRS})
    target_link_libraries(${E_NAME} PRIVATE gar-grin Threads::Threads ${CMAKE_DL_LIBS} ${Protobuf_LIBRARIES})

    # if OpenSSL library exists, link the OpenSSL library.
    # OpenSSL has to be linked after GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
    if(OPENSSL_FOUND)
        target_link_libraries(${E_NAME} PRIVATE OpenSSL::SSL)
    endif()
endforeach()