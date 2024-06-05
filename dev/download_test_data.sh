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

# A script to download test data for GraphAr

if [ -n "${GAR_TEST_DATA}" ]; then
    if [[ ! -d "$GAR_TEST_DATA" ]]; then
        echo "GAR_TEST_DATA is set but the directory does not exist, cloning the test data to $GAR_TEST_DATA"
        git clone https://github.com/apache/incubator-graphar-testing.git "$GAR_TEST_DATA" --depth 1 || true
    fi
else
    echo "GAR_TEST_DATA is not set, cloning the test data to /tmp/graphar-testing"
    git clone https://github.com/apache/incubator-graphar-testing.git /tmp/graphar-testing --depth 1 || true
    echo "Test data has been cloned to /tmp/graphar-testing, please run"
    echo "    export GAR_TEST_DATA=/tmp/graphar-testing"
fi

