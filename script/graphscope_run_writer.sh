#!/bin/bash
set -eo pipefail
export VINEYARD_HOME=./bin
# for open-mpi
export OMPI_MCA_btl_vader_single_copy_mechanism=none
export OMPI_MCA_orte_allowed_exit_without_sync=1

np=1
cmd_prefix="GLOG_minloglevel=0 mpirun"
if ompi_info; then
  echo "Using openmpi"
  cmd_prefix="${cmd_prefix} --allow-run-as-root"
fi

socket_file=/tmp/vineyard.sock
bin_home=./bin
test_dir=/mnt/dataset/raw
graph_yaml=/mnt/dataset/cf/cf.graph.yml_18_22
function start_vineyard() {
  pkill vineyardd || true
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

efiles="'${test_dir}/com-friendster.e#src_label=person&dst_label=person&label=knows#schema=src,dst#column_types=int64,int64#header_row=true#delimiter= '"

vfiles="'${test_dir}/com-friendster.v#label=person#schema=id#column_types=int64#header_row=true#delimiter= '"

function run_lpa() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift
  e_label_num=$1
  shift
  v_label_num=$1
  shift

#  cmd="${cmd_prefix} -n ${num_procs} --host h0:1,h1:1,h2:1,h3:1 ${executable} ${socket_file}"
 cmd="${cmd_prefix} -n ${num_procs} --host h0:${num_procs} ${executable} ${socket_file}"

  cmd="${cmd} ${e_label_num}"
  cmd="${cmd} ${efiles}"
  cmd="${cmd} ${v_label_num}"
  cmd="${cmd} ${vfiles}"

  cmd="${cmd} $*"

  echo "${cmd}"
  eval "${cmd}"
  echo "Finished running lpa on property graph."
}

function run_gar_simple() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift
  e_label_num=$1
  shift
  e_prefix=$1
  shift
  v_label_num=$1
  shift
  v_prefix=$1
  shift

  cmd="${cmd_prefix} -n ${num_procs} --host localhost:${num_procs} ${executable} ${socket_file}"

  cmd="${cmd} ${e_label_num}"
  for ((i = 0; i < e_label_num; i++)); do
    cmd="${cmd} '${e_prefix}_${i}.csv#src_label=person&dst_label=person&label=knows#delimiter=|'"
  done

  cmd="${cmd} ${v_label_num}"
  for ((i = 0; i < v_label_num; i++)); do
    cmd="${cmd} '${v_prefix}_${i}.csv#label=person#delimiter=|'"
  done

  cmd="${cmd} $*"

  echo "${cmd}"
  eval "${cmd}"
  echo "Finished running lpa on property graph."
}

function run_gar() {
  num_procs=$1
  shift
  executable=$1
  shift
  socket_file=$1
  shift
  e_label_num=$1
  shift
  v_label_num=$1
  shift

  cmd="${cmd_prefix} -n ${num_procs} --host localhost:${num_procs} ${executable} ${socket_file}"

  cmd="${cmd} ${e_label_num}"
  cmd="${cmd} ${efiles}"
  cmd="${cmd} ${v_label_num}"
  cmd="${cmd} ${vfiles}"

  cmd="${cmd} $*"

  echo "${cmd}"
  eval "${cmd}"
  echo "Finished running lpa on property graph."
}
start_vineyard

run_lpa ${np} ${bin_home}/arrow_fragment_writer_test "${socket_file}" 1 1 "${graph_yaml}" 1

# run_gar_simple ${np} ${bin_home}/arrow_fragment_writer_test "${socket_file}" 1 "${test_dir}/ldbc_sample/person_knows_person_0" 1 "${test_dir}/ldbc_sample/person_0"  "${graph_yaml}" 1
