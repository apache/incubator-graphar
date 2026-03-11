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

find_package(Boost REQUIRED COMPONENTS graph)

file(GLOB EXAMPLE_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
foreach(f ${EXAMPLE_FILES})
    string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
    set(E_NAME ${CMAKE_MATCH_1})
    message(STATUS "Found example - " ${E_NAME})
    add_executable(${E_NAME} ${E_NAME}.cc)
    target_include_directories(${E_NAME} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/thirdparty
    )
    target_include_directories(${E_NAME} SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
    target_link_libraries(${E_NAME} PRIVATE graphar ${Boost_LIBRARIES} ${CMAKE_DL_LIBS})
    
    if(BUILD_ARROW_FROM_SOURCE)
        target_include_directories(${E_NAME} SYSTEM BEFORE PRIVATE ${GAR_ARROW_INCLUDE_DIR})
        if(APPLE)
            target_link_libraries(${E_NAME} PRIVATE -Wl,-force_load ${GAR_ARROW_STATIC_LIB}
                "${GAR_PARQUET_STATIC_LIB}"
                "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}"
                protobuf::libprotobuf
                "-framework CoreFoundation"
                "-framework Security"
                "-framework Network")
        else()
            target_link_libraries(${E_NAME} PRIVATE -Wl,--exclude-libs,ALL -Wl,--whole-archive ${GAR_ARROW_STATIC_LIB}
                "${GAR_PARQUET_STATIC_LIB}"
                "${GAR_ARROW_BUNDLED_DEPS_STATIC_LIB}" -Wl,--no-whole-archive)
        endif()
    else()
        if(APPLE)
            if(USE_STATIC_ARROW)
                target_link_libraries(${E_NAME} PRIVATE -Wl,-force_load
                    Arrow::arrow_static
                    Parquet::parquet_static)
            else()
                target_link_libraries(${E_NAME} PRIVATE Arrow::arrow_shared
                    Parquet::parquet_shared)
            endif()
        else()
            if(USE_STATIC_ARROW)
                target_link_libraries(${E_NAME} PRIVATE -Wl,--exclude-libs,ALL -Wl,--whole-archive 
                    Arrow::arrow_static 
                    Parquet::parquet_static -Wl,--no-whole-archive)
            else()
                target_link_libraries(${E_NAME} PRIVATE Arrow::arrow_shared
                    Parquet::parquet_shared)
            endif()
        endif()
    endif()
endforeach()
