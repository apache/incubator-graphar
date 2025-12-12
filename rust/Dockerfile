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

# Derived from Graphar
# https://github.com/apache/incubator-graphar/blob/main/.devcontainer/Dockerfile

# A script to install dependencies required for release
# verification on Ubuntu.
FROM ubuntu:24.04

RUN apt-get update && 				                  \
    apt-get install -y --no-install-recommends 	\
      build-essential         			            \
      cmake                   			            \
      curl                    			            \
      wget 					                            \
      git ca-certificates   		      	        \
      lsb-release                               \
      clang clangd clang-format vim 

RUN wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb    \
    && apt-get install -y ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb                                                                        \
    && apt-get update                                                                                                                                                   \
    && apt-get install -y                                                                                                                                               \
        libarrow-dev                                                                                                                                                    \
        libarrow-dataset-dev                                                                                                                                            \
        libarrow-acero-dev                                                                                                                                              \
        libparquet-dev                                                                                                                                                  \
        libarrow-compute-dev                                                                                                                                            \
        libboost-graph-dev

RUN apt-get clean -y &&  				            \
    rm -rf /var/lib/apt/lists/* ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb

# install rust toolchain
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

WORKDIR /workspace
