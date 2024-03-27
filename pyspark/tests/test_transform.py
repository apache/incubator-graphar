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

# copyright 2022-2023 alibaba group holding limited.
#
# licensed under the apache license, version 2.0 (the "license");
# you may not use this file except in compliance with the license.
# you may obtain a copy of the license at
#
#     http://www.apache.org/licenses/license-2.0
#
# unless required by applicable law or agreed to in writing, software
# distributed under the license is distributed on an "as is" basis,
# without warranties or conditions of any kind, either express or implied.
# see the license for the specific language governing permissions and
# limitations under the license.

from pathlib import Path

import pytest
from graphar_pyspark import initialize
from graphar_pyspark.errors import InvalidGraphFormatError
from graphar_pyspark.graph import GraphTransformer
from graphar_pyspark.info import GraphInfo

GRAPHAR_TESTS_EXAMPLES = Path(__file__).parent.parent.parent.joinpath("testing")


def test_transform(spark):
    initialize(spark)
    source_path = (
        GRAPHAR_TESTS_EXAMPLES.joinpath("ldbc_sample/parquet/ldbc_sample.graph.yml")
        .absolute()
        .__str__()
    )
    dest_path = (
        GRAPHAR_TESTS_EXAMPLES.joinpath("transformer/ldbc_sample.graph.yml")
        .absolute()
        .__str__()
    )
    GraphTransformer.transform(source_path, dest_path)

    source_info = GraphInfo.load_graph_info(source_path)
    dest_info = GraphInfo.load_graph_info(dest_path)

    with pytest.raises(InvalidGraphFormatError):
        GraphTransformer.transform(source_path, source_info)

    GraphTransformer.transform(source_info, dest_info)
