#!/bin/bash

# ./run-bi-2 tag_hasType_tagClass_path_file post_hasTag_tag_path_file comment_hasTag_tag_path_file post_tagClass_label_path_file comment_tagClass_label_path_file comment_vertex_path_file post_vertex_path_file tag_vertex_path_file tag_class_id date vertex_num_tag vertex_num_post vertex_num_comment

echo 'sf-30'
./run-bi-2 /root/dataset/sf-30/bi/tag_hasType_tagclass_0_0 /root/dataset/sf-30/bi/post_hasTag_tag_0_0 /root/dataset/sf-30/bi/comment_hasTag_tag_0_0 /root/dataset/sf-30/bi/post_parquet_graphar_label_RLE.parquet /root/dataset/sf-30/bi/comment_parquet_graphar_label_RLE.parquet /root/dataset/sf-30/bi/comment_0_0.parquet /root/dataset/sf-30/bi/post_0_0.parquet /root/dataset/sf-30/bi/tag_0_0.parquet MusicalArtist 18 1338508800000 16080 19968658 67126524

echo 'sf-100'
./run-bi-2 /root/dataset/sf-100/bi/tag_hasType_tagclass_0_0 /root/dataset/sf-100/bi/post_hasTag_tag_0_0 /root/dataset/sf-100/bi/comment_hasTag_tag_0_0 /root/dataset/sf-100/bi/post_parquet_graphar_label_RLE.parquet /root/dataset/sf-100/bi/comment_parquet_graphar_label_RLE.parquet /root/dataset/sf-100/bi/comment_0_0.parquet /root/dataset/sf-100/bi/post_0_0.parquet /root/dataset/sf-100/bi/tag_0_0.parquet MusicalArtist 18 1338508800000 16080 57987023 220096052

echo 'sf-300'
./run-bi-2 /root/dataset/sf-300/bi/tag_hasType_tagclass_0_0 /root/dataset/sf-300/bi/post_hasTag_tag_0_0 /root/dataset/sf-300/bi/comment_hasTag_tag_0_0 /root/dataset/sf-300/bi/post_parquet_graphar_label_RLE.parquet /root/dataset/sf-300/bi/comment_parquet_graphar_label_RLE.parquet /root/dataset/sf-300/bi/comment_0_0.parquet /root/dataset/sf-300/bi/post_0_0.parquet /root/dataset/sf-300/bi/tag_0_0.parquet MusicalArtist 18 1338508800000 16080 155783470 650086949
