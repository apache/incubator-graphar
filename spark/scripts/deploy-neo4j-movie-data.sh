#!/bin/bash

set -eu

curl https://raw.githubusercontent.com/neo4j-graph-examples/movies/main/data/movies-43.dump -o ${NEO4J_HOME}/movies-43.dump
neo4j-admin load --from ${NEO4J_HOME}/movies-43.dump --database=neo4j
neo4j start