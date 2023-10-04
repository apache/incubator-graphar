#!/bin/bash

set -eu

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
jar_file="${cur_dir}/../target/graphar-0.1.0-SNAPSHOT-shaded.jar"
conf_path="$(readlink -f $1)"

spark-submit --class com.alibaba.graphar.importer.Neo4j ${jar_file} \
    ${conf_path}