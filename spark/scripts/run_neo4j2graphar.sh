#!/bin/bash

set -eu

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
jar_file="${cur_dir}/../target/graphar-0.1.0-SNAPSHOT-shaded.jar"

vertex_chunk_size=100
edge_chunk_size=1024
file_type="csv"
spark-submit --class com.alibaba.graphar.example.Neo4j2GraphAr ${jar_file} \
    "/tmp/graphar/neo4j2graphar" ${vertex_chunk_size} ${edge_chunk_size} ${file_type}
