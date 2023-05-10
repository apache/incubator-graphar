# build GraphAr-grin

find_package(Protobuf REQUIRED)

file(GLOB_RECURSE GAR_GRIN_SRC_FILES "src/*.cc")

message(${GAR_GRIN_SRC_FILES})

add_library(gar-grin SHARED ${GAR_GRIN_SRC_FILES})

target_compile_features(gar-grin PUBLIC cxx_std_17)

target_include_directories(gar-grin SYSTEM BEFORE PUBLIC include ${Protobuf_INCLUDE_DIRS})

target_link_libraries(gar-grin PUBLIC gar ${Protobuf_LIBRARIES})

# ------------------------------------------------------------------------------
# complile protobuf to generate cpp files
# ------------------------------------------------------------------------------
set(DST_DIR "${PROJECT_SOURCE_DIR}/grin/src/proto/")

list(APPEND PROTO_FLAGS -I${PROJECT_SOURCE_DIR}/grin/include/proto/)

file(GLOB PROTO_FILES RELATIVE "${PROJECT_SOURCE_DIR}/grin/include/proto" "${PROJECT_SOURCE_DIR}/grin/include/proto/*.proto")
set(PROTO_SRCS "")
set(PROTO_HDRS "")

foreach(f ${PROTO_FILES})
    get_filename_component(FIL_WE ${f} NAME_WE)
    list(APPEND PROTO_SRCS "${DST_DIR}/${FIL_WE}.pb.cc")
    list(APPEND PROTO_HDRS "${DST_DIR}/${FIL_WE}.pb.h")

    execute_process(COMMAND ${Protobuf_PROTOC_EXECUTABLE} ${PROTO_FLAGS} --cpp_out=${DST_DIR} ${f})
endforeach()

set_source_files_properties(${PROTO_SRCS} ${PROTO_HDRS} PROPERTIES GENERATED TRUE)

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
# build C tests
# ------------------------------------------------------------------------------
file(GLOB C_TEST_FILES RELATIVE "${PROJECT_SOURCE_DIR}/grin/c_test" "${PROJECT_SOURCE_DIR}/grin/c_test/*.c")

foreach(f ${C_TEST_FILES})
    string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
    set(E_NAME ${CMAKE_MATCH_1})
    message(STATUS "Found C test - " ${E_NAME})
    add_executable(C_${E_NAME} c_test/${E_NAME}.c)
    target_include_directories(C_${E_NAME} SYSTEM BEFORE PRIVATE ${PROJECT_SOURCE_DIR}/include ${Protobuf_INCLUDE_DIRS})
    target_link_libraries(C_${E_NAME} PRIVATE gar-grin Threads::Threads ${CMAKE_DL_LIBS} ${Protobuf_LIBRARIES})

    # if OpenSSL library exists, link the OpenSSL library.
    # OpenSSL has to be linked after GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
    if(OPENSSL_FOUND)
        target_link_libraries(C_${E_NAME} PRIVATE OpenSSL::SSL)
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
