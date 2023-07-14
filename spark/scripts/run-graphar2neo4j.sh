#!/bin/bash

set -eu

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
jar_file="${cur_dir}/../target/graphar-0.1.0-SNAPSHOT-shaded.jar"

graph_info_path="${GRAPH_INFO_PATH:-/tmp/movie_graph/MovieGraph.graph.yml}"
spark-submit --class com.alibaba.graphar.example.GraphAr2Neo4j ${jar_file} \
    ${graph_info_path}
