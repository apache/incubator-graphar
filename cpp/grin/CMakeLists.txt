# build GraphAr-grin

file(GLOB_RECURSE GAR_GRIN_SRC_FILES "src/*.cc")

message(${GAR_GRIN_SRC_FILES})

add_library(gar-grin SHARED ${GAR_GRIN_SRC_FILES})

target_compile_features(gar-grin PUBLIC cxx_std_17)

target_include_directories(gar-grin PUBLIC include)

target_link_libraries(gar-grin PUBLIC gar)

# ------------------------------------------------------------------------------
# build example
# ------------------------------------------------------------------------------
file(GLOB EXAMPLE_FILES RELATIVE "${PROJECT_SOURCE_DIR}/grin/example" "${PROJECT_SOURCE_DIR}/grin/example/*.cc")

foreach(f ${EXAMPLE_FILES})
    string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
    set(E_NAME ${CMAKE_MATCH_1})
    message(STATUS "Found example - " ${E_NAME})
    add_executable(${E_NAME} example/${E_NAME}.cc)
    target_include_directories(${E_NAME} PRIVATE ${PROJECT_SOURCE_DIR}/include)
    target_link_libraries(${E_NAME} PRIVATE gar-grin Threads::Threads ${CMAKE_DL_LIBS})

    # if OpenSSL library exists, link the OpenSSL library.
    # OpenSSL has to be linked after GAR_ARROW_BUNDLED_DEPS_STATIC_LIB
    if(OPENSSL_FOUND)
        target_link_libraries(${E_NAME} PRIVATE OpenSSL::SSL)
    endif()
endforeach()