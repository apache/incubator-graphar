# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

file(GLOB_RECURSE THIRDPARTY_SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/mini-yaml/yaml/*.cpp)
add_library(graphar_thirdparty STATIC ${THIRDPARTY_SRC_FILES})
target_include_directories(graphar_thirdparty PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty)
target_compile_features(graphar_thirdparty PRIVATE cxx_std_17)
if(NOT MSVC)
    target_compile_options(graphar_thirdparty PRIVATE -fPIC -w)
endif()

file(GLOB_RECURSE CORE_SRC_FILES "graphar/*.cc")
if(GRAPHAR_BUILD_STATIC)
    add_library(graphar STATIC ${CORE_SRC_FILES})
else()
    add_library(graphar SHARED ${CORE_SRC_FILES})
endif()
install(TARGETS graphar graphar_thirdparty
        EXPORT graphar-targets
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
        INCLUDES DESTINATION include
)
target_compile_features(graphar PRIVATE cxx_std_${GAR_CXX_STANDARD})
target_include_directories(graphar PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty)
if(GRAPHAR_BUILD_STATIC)
    target_link_libraries(graphar PUBLIC graphar_thirdparty ${CMAKE_DL_LIBS})
else()
    target_link_libraries(graphar PRIVATE graphar_thirdparty ${CMAKE_DL_LIBS})
endif()

if(BUILD_ARROW_FROM_SOURCE)
    target_include_directories(graphar SYSTEM BEFORE PRIVATE ${GAR_ARROW_INCLUDE_DIR})

    set(GAR_BUNDLED_DEPS_STATIC_LIBS)
    list(APPEND GAR_BUNDLED_DEPS_STATIC_LIBS
        gar_arrow_static
        gar_arrow_compute_static
        gar_parquet_static
        gar_dataset_static
        gar_acero_static
        gar_arrow_bundled_dependencies_static)
    graphar_car(_FIRST_LIB ${GAR_BUNDLED_DEPS_STATIC_LIBS})
    graphar_cdr(_OTHER_LIBS ${GAR_BUNDLED_DEPS_STATIC_LIBS})

    graphar_create_merged_static_lib(graphar_bundled_dependencies
                                    NAME
                                    graphar_bundled_dependencies
                                    ROOT
                                    ${_FIRST_LIB}
                                    TO_MERGE
                                    ${_OTHER_LIBS})
    get_target_property(graphar_bundled_dependencies_path graphar_bundled_dependencies
                        IMPORTED_LOCATION)
    install(FILES ${CMAKE_BINARY_DIR}/${graphar_bundled_dependencies_path} ${INSTALL_IS_OPTIONAL}
            DESTINATION ${CMAKE_INSTALL_LIBDIR})
    string(APPEND ARROW_PC_LIBS_PRIVATE " -lgraphar_bundled_dependencies")
    list(INSERT ARROW_STATIC_INSTALL_INTERFACE_LIBS 0 "graphar_bundled_dependencies")

    if(APPLE)
        find_package(Protobuf REQUIRED)
        target_link_libraries(graphar PRIVATE -Wl,-force_load
                graphar_bundled_dependencies
                protobuf::libprotobuf
                "-framework CoreFoundation"
                "-framework Security"
                "-framework Network")
    elseif(MSVC)
        target_link_libraries(graphar PRIVATE graphar_bundled_dependencies)
    else()
        target_link_libraries(graphar PRIVATE -Wl,--exclude-libs,ALL
                graphar_bundled_dependencies)
    endif()

    if(OPENSSL_FOUND)
        target_link_libraries(graphar PUBLIC OpenSSL::SSL)
    endif()
    if(CURL_FOUND)
        target_link_libraries(graphar PUBLIC ${CURL_LIBRARIES})
    endif()
else()
    if(APPLE)
        if(USE_STATIC_ARROW)
            target_link_libraries(graphar PRIVATE -Wl,-force_load
                Arrow::arrow_static
                Parquet::parquet_static
                ArrowDataset::arrow_dataset_static
                ArrowAcero::arrow_acero_static)
        else()
            target_link_libraries(graphar PRIVATE -Wl,-force_load Arrow::arrow_shared
                Parquet::parquet_shared
                ArrowDataset::arrow_dataset_shared
                ArrowAcero::arrow_acero_shared)
        endif()
    elseif(MSVC)
        if(USE_STATIC_ARROW)
            target_link_libraries(graphar PRIVATE
                Arrow::arrow_static
                Parquet::parquet_static
                ArrowDataset::arrow_dataset_static
                ArrowAcero::arrow_acero_static)
        else()
            target_link_libraries(graphar PRIVATE
                Arrow::arrow_shared
                Parquet::parquet_shared
                ArrowDataset::arrow_dataset_shared
                ArrowAcero::arrow_acero_shared)
        endif()
    else()
        if(USE_STATIC_ARROW)
            target_link_libraries(graphar PRIVATE -Wl,--exclude-libs,ALL -Wl,--whole-archive
                Arrow::arrow_static
                Parquet::parquet_static
                ArrowDataset::arrow_dataset_static
                ArrowAcero::arrow_acero_static -Wl,--no-whole-archive)
        else()
            target_link_libraries(graphar PRIVATE -Wl,--exclude-libs,ALL -Wl,--whole-archive Arrow::arrow_shared
                Parquet::parquet_shared
                ArrowDataset::arrow_dataset_shared
                ArrowAcero::arrow_acero_shared -Wl,--no-whole-archive)
        endif()
    endif()
endif()
