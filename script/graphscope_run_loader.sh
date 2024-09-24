#!/bin/bash
set -eo pipefail
export VINEYARD_HOME=./bin
# for open-mpi
export OMPI_MCA_btl_vader_single_copy_mechanism=none
export OMPI_MCA_orte_allowed_exit_without_sync=1

np=4
cmd_prefix="GLOG_v=111 mpirun"
if ompi_info; then
  echo "Using openmpi"
  cmd_prefix="${cmd_prefix} --allow-run-as-root"
fi

socket_file=/tmp/vineyard.sock
bin_home=./bin
test_dir=/home/graphscope/v6d/gstest
graph_yaml=/mnt/dataset/cf/cf.graph.yml_18_22
function start_vineyard() {
  pkill vineyardd ||
  pkill etcd || true
  rm ${socket_file} || true
  echo "[INFO] vineyardd will using the socket_file on ${socket_file}"

  timestamp=$(date +%Y-%m-%d_%H-%M-%S)
  ${VINEYARD_HOME}/vineyardd \
    --socket ${socket_file} \
    --size 161061273600 \
    --etcd_prefix "${timestamp}" \
    --etcd_endpoint=http://127.0.0.1:3457 &
  set +m
  sleep 5
  echo "vineyardd started."
}

function run_loader_gar() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift

  cmd="${cmd_prefix} -n ${num_procs} --host localhost:${num_procs} ${executable} ${socket_file}"

  cmd="${cmd} $*"

  echo "${cmd}"
  eval "${cmd}"
  echo "Finished running lpa on property graph."
}
start_vineyard

run_loader_gar ${np} ${bin_home}/arrow_fragment_loader_test "${socket_file}" "${graph_yaml}" 1
