# build GraphAr-grin

find_package(Protobuf REQUIRED)

find_package(glog REQUIRED)

file(GLOB_RECURSE GAR_GRIN_SRC_FILES "src/*.cc")

message(${GAR_GRIN_SRC_FILES})

add_library(gar-grin SHARED ${GAR_GRIN_SRC_FILES})

target_compile_features(gar-grin PUBLIC cxx_std_17)

target_include_directories(gar-grin SYSTEM BEFORE PUBLIC ${PROJECT_SOURCE_DIR}/thirdparty/grin/include ${Protobuf_INCLUDE_DIRS})

target_link_libraries(gar-grin PUBLIC gar ${Protobuf_LIBRARIES} glog::glog)

# ------------------------------------------------------------------------------
# complile protobuf to generate cpp files
# ------------------------------------------------------------------------------
set(DST_DIR "${PROJECT_SOURCE_DIR}/grin/src/common/")

list(APPEND PROTO_FLAGS -I${PROJECT_SOURCE_DIR}/thirdparty/grin/proto/)

file(GLOB PROTO_FILES RELATIVE "${PROJECT_SOURCE_DIR}/thirdparty/grin/proto" "${PROJECT_SOURCE_DIR}/thirdparty/grin/proto/*.proto")

set(PROTO_SRCS "")
set(PROTO_HDRS "")

foreach(f ${PROTO_FILES})
    message(STATUS "Found proto - " ${f})
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
    message(STATUS "Found GRIN test - " ${E_NAME})
    add_executable(${E_NAME} test/${E_NAME}.cc)
    target_include_directories(${E_NAME} SYSTEM BEFORE PRIVATE ${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/thirdparty/grin/include ${Protobuf_INCLUDE_DIRS})
    target_link_libraries(${E_NAME} PRIVATE gar-grin Threads::Threads ${CMAKE_DL_LIBS} ${Protobuf_LIBRARIES} glog::glog)

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
    message(STATUS "Found GRIN C test - " ${E_NAME})
    add_executable(C_${E_NAME} c_test/${E_NAME}.c)
    target_include_directories(C_${E_NAME} SYSTEM BEFORE PRIVATE ${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/thirdparty/grin/include ${Protobuf_INCLUDE_DIRS})
    target_link_libraries(C_${E_NAME} PRIVATE gar-grin Threads::Threads ${CMAKE_DL_LIBS} ${Protobuf_LIBRARIES} glog::glog)

    # if OpenSSL library exists, link the OpenSSL library.
    # OpenSSL has to be linked after GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
    if(OPENSSL_FOUND)
        target_link_libraries(C_${E_NAME} PRIVATE OpenSSL::SSL)
    endif()
endforeach()

# ------------------------------------------------------------------------------
# build examples
# ------------------------------------------------------------------------------
if(USE_LTO AND CMAKE_VERSION VERSION_GREATER "3.9" AND CMAKE_BUILD_TYPE STREQUAL "Release")
    include(CheckIPOSupported)
    check_ipo_supported(RESULT lpo_supported OUTPUT lpo_supported_error)
    if(lpo_supported)
        message(STATUS "IPO / LTO enabled")
        # set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    else()
        message(STATUS "IPO / LTO not supported: '${lpo_supported_error}'")
    endif()
endif()

find_package(MPI REQUIRED)

file(GLOB WITH_GRIN_EXAMPLE_FILES RELATIVE "${PROJECT_SOURCE_DIR}/grin/example" "${PROJECT_SOURCE_DIR}/grin/example/*_with_grin.cc")

file(GLOB WITHOUT_GRIN_EXAMPLE_FILES RELATIVE "${PROJECT_SOURCE_DIR}/grin/example" "${PROJECT_SOURCE_DIR}/grin/example/*_without_grin.cc")

# build examples with grin
foreach(f ${WITH_GRIN_EXAMPLE_FILES})
    string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
    set(E_NAME ${CMAKE_MATCH_1})
    message(STATUS "Found GRIN example - " ${E_NAME})
    add_executable(${E_NAME} example/${E_NAME}.cc ${GAR_GRIN_SRC_FILES})
    target_include_directories(${E_NAME} SYSTEM BEFORE PRIVATE ${PROJECT_SOURCE_DIR}/include ${PROJECT_SOURCE_DIR}/thirdparty/grin/include ${Protobuf_INCLUDE_DIRS} ${MPI_INCLUDE_PATH})
    # enable LTO for building examples
    set_property(TARGET ${E_NAME} PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    # link to gar instead of gar-grin for performance
    target_link_libraries(${E_NAME} PRIVATE gar Threads::Threads ${CMAKE_DL_LIBS} ${Protobuf_LIBRARIES} glog::glog ${MPI_LIBRARIES})
    # target_link_libraries(${E_NAME} PRIVATE gar-grin Threads::Threads ${CMAKE_DL_LIBS} ${Protobuf_LIBRARIES} glog::glog ${MPI_LIBRARIES})

    # if OpenSSL library exists, link the OpenSSL library.
    # OpenSSL has to be linked after GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
    if(OPENSSL_FOUND)
        target_link_libraries(${E_NAME} PRIVATE OpenSSL::SSL)
    endif()
endforeach()

# build examples without grin
foreach(f ${WITHOUT_GRIN_EXAMPLE_FILES})
    string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
    set(E_NAME ${CMAKE_MATCH_1})
    message(STATUS "Found GRIN example - " ${E_NAME})
    add_executable(${E_NAME} example/${E_NAME}.cc)
    target_include_directories(${E_NAME} SYSTEM BEFORE PRIVATE ${PROJECT_SOURCE_DIR}/include ${Protobuf_INCLUDE_DIRS} ${MPI_INCLUDE_PATH})
    # enable LTO for building examples
    set_property(TARGET ${E_NAME} PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    # link to gar
    target_link_libraries(${E_NAME} PRIVATE gar Threads::Threads ${CMAKE_DL_LIBS} ${Protobuf_LIBRARIES} glog::glog ${MPI_LIBRARIES})

    # if OpenSSL library exists, link the OpenSSL library.
    # OpenSSL has to be linked after GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
    if(OPENSSL_FOUND)
        target_link_libraries(${E_NAME} PRIVATE OpenSSL::SSL)
    endif()
endforeach()
