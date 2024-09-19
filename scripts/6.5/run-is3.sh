#!/bin/bash

# ./run-work-load edge_file_prefix vertex_file vertex_num vertex_id delta/base

echo 'sf-30'
./run-work-load /root/dataset/sf-30/person_knows_person /root/dataset/sf-30/person_knows_person-vertex-base 165430 17592186044461 base
./run-work-load /root/dataset/sf-30/person_knows_person /root/dataset/sf-30/person_knows_person-vertex-base 165430 70220 delta
echo 'sf-100'
./run-work-load /root/dataset/sf-100/person_knows_person /root/dataset/sf-100/person_knows_person-vertex-base 448626 17592186044461 base
./run-work-load /root/dataset/sf-100/person_knows_person /root/dataset/sf-100/person_knows_person-vertex-base 448626 191086 delta
echo 'sf-300'
./run-work-load /root/dataset/sf-300/person_knows_person /root/dataset/sf-300/person_knows_person-vertex-base 1128069 17592186044461 base
./run-work-load /root/dataset/sf-300/person_knows_person /root/dataset/sf-300/person_knows_person-vertex-base 1128069 479964 delta
