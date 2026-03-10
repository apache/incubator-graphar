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

# - Config file for the graphar package
#
# It defines the following variables
#
#  GRAPHAR_INCLUDE_DIR         - include directory for graphar
#  GRAPHAR_INCLUDE_DIRS        - include directories for graphar
#  GRAPHAR_LIBRARIES           - libraries to link against

@PACKAGE_INIT@

include(CMakeFindDependencyMacro)

if(NOT @BUILD_ARROW_FROM_SOURCE@)
    find_dependency(Arrow REQUIRED)
    find_dependency(Parquet REQUIRED)
    find_dependency(ArrowDataset REQUIRED)
    if(@Arrow_VERSION@ VERSION_GREATER_EQUAL "12.0.0")
        find_dependency(ArrowAcero)
    endif()
endif()

include("${CMAKE_CURRENT_LIST_DIR}/graphar-targets.cmake")

set(GRAPHAR_LIBRARIES graphar::graphar)
get_target_property(GRAPHAR_INCLUDE_DIRS graphar::graphar INTERFACE_INCLUDE_DIRECTORIES)
set(GRAPHAR_INCLUDE_DIR ${GRAPHAR_INCLUDE_DIRS})
