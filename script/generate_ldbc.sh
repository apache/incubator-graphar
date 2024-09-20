#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: \$0 <edge_path> <vertex_path> <output_prefix>"
    exit 1
fi

EDGE_PATH=$1
VERTEX_PATH=$2
OUTPUT_PREFIX=$3

../build/release/Csv2Parquet-ldbc "$EDGE_PATH" "$OUTPUT_PREFIX" 0 edge to_undirected

../build/release/Csv2Parquet-ldbc "$VERTEX_PATH" "$OUTPUT_PREFIX" 0 vertex

../build/release/data-generator-label "$EDGE_PATH" "$VERTEX_PATH" "$VERTEX_PATH" person person csv csv "$OUTPUT_PREFIX" not-reverse to_undirected

echo "done!"
