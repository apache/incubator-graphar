#!/bin/bash

set -eu
cd "$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

curl https://dist.neo4j.org/neo4j-community-4.4.23-unix.tar.gz | tar -xz -C ${HOME}/
