#!/bin/bash
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <GRAPH_PATH> <VERTEX_NUM> <SOURCE_VERTEX>"
    exit 1
fi

GRAPH_PATH=$1
VERTEX_NUM=$2
SOURCE_VERTEX=$3

# Print the graph path
echo "GRAPH_PATH: $GRAPH_PATH"
echo "VERTEX_NUM: $VERTEX_NUM"
echo "SOURCE_VERTEX: $SOURCE_VERTEX"

../build/release/decode-bit-map "$GRAPH_PATH" "$VERTEX_NUM" "$SOURCE_VERTEX" delta
../build/release/decode-bit-map "$GRAPH_PATH" "$VERTEX_NUM" "$SOURCE_VERTEX" base
../build/release/decode-bit-map "$GRAPH_PATH" "$VERTEX_NUM" "$SOURCE_VERTEX"