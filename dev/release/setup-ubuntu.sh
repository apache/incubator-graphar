#!/bin/bash
#
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

# A script to install dependencies required for release
# verification on Ubuntu.

set -exu

codename=$(. /etc/os-release && echo ${UBUNTU_CODENAME})
id=$(. /etc/os-release && echo ${ID})

apt-get install -y -q --no-install-recommends \
  build-essential \
  cmake \
  git \
  gnupg \
  libcurl4-openssl-dev \
  maven \
  openjdk-11-jdk \
  wget \
  pkg-config \
  tzdata \
  subversion

wget -c https://apache.jfrog.io/artifactory/arrow/${id}/apache-arrow-apt-source-latest-${codename}.deb \
    -P /tmp/
apt-get install -y -q /tmp/apache-arrow-apt-source-latest-${codename}.deb
apt-get update -y -q
apt-get install -y -q --no-install-recommends \
  libarrow-dev \
  libarrow-dataset-dev \
  libarrow-acero-dev \
  libparquet-dev
