#!/bin/bash

# acero:
# ./run-ic-8 post_has_creator_person_path_file comment_has_creator_person_path_file comment_replyof_post_path_file comment_replyof_comment_path_file comment_vertex_file person_vertex_file vertex_id post_vertex_num comment_vertex_num person_vertex_num
# graphar 
# ./run-ic-8-alter post_has_creator_person_path_file comment_has_creator_person_path_file comment_replyof_post_path_file comment_replyof_comment_path_file comment_vertex_file person_vertex_file vertex_id post_vertex_num comment_vertex_num person_vertex_num

echo 'sf-30'
./run-ic-8 /root/dataset/sf-30/post_hasCreator_person_0_0 /root/dataset/sf-30/comment_hasCreator_person_0_0 /root/dataset/sf-30/comment_replyOf_post_0_0 /root/dataset/sf-30/comment_replyOf_comment_0_0 /root/dataset/sf-30/comment_0_0.parquet /root/dataset/sf-30/person_0_0.parquet 143 19968658 67126524 165430
./run-ic-8-alter /root/dataset/sf-30/post_hasCreator_person_0_0 /root/dataset/sf-30/comment_hasCreator_person_0_0 /root/dataset/sf-30/comment_replyOf_post_0_0 /root/dataset/sf-30/comment_replyOf_comment_0_0 /root/dataset/sf-30/comment_0_0_index.parquet /root/dataset/sf-30/person_0_0_index.parquet 95950 19968658 67126524 165430

echo 'sf-100'
./run-ic-8 /root/dataset/sf-100/post_hasCreator_person_0_0 /root/dataset/sf-100/comment_hasCreator_person_0_0 /root/dataset/sf-100/comment_replyOf_post_0_0 /root/dataset/sf-100/comment_replyOf_comment_0_0 /root/dataset/sf-100/comment_0_0.parquet /root/dataset/sf-100/person_0_0.parquet 143 57987023 220096052 448626
./run-ic-8-alter /root/dataset/sf-100/post_hasCreator_person_0_0 /root/dataset/sf-100/comment_hasCreator_person_0_0 /root/dataset/sf-100/comment_replyOf_post_0_0 /root/dataset/sf-100/comment_replyOf_comment_0_0 /root/dataset/sf-100/comment_0_0_index.parquet /root/dataset/sf-100/person_0_0_index.parquet 260823 57987023 220096052 448626

echo 'sf-300'
./run-ic-8 /root/dataset/sf-300/post_hasCreator_person_0_0 /root/dataset/sf-300/comment_hasCreator_person_0_0 /root/dataset/sf-300/comment_replyOf_post_0_0 /root/dataset/sf-300/comment_replyOf_comment_0_0 /root/dataset/sf-300/comment_0_0.parquet /root/dataset/sf-300/person_0_0.parquet 143 155783470 650086949 1128069
./run-ic-8-alter /root/dataset/sf-300/post_hasCreator_person_0_0 /root/dataset/sf-300/comment_hasCreator_person_0_0 /root/dataset/sf-300/comment_replyOf_post_0_0 /root/dataset/sf-300/comment_replyOf_comment_0_0 /root/dataset/sf-300/comment_0_0_index.parquet /root/dataset/sf-300/person_0_0_index.parquet 655084 155783470 650086949 1128069
