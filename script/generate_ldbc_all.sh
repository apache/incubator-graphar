#!/bin/bash
# Generate the base dataset
# ./Csv2Parquet-is3 sourcefile output_file_prefix ignore_rows type [to_undirected]
# sf30
./Csv2Parquet-is3 /root/dataset/sf30/social_network/dynamic/person_knows_person_0_0.csv /root/dataset/sf30/parquet/person_knows_person 0 edge to_undirected
./Csv2Parquet-is3 /root/dataset/sf30/social_network/dynamic/person_0_0.csv /root/dataset/sf30/person_knows_person 0 vertex
# sf100
./Csv2Parquet-is3 /root/dataset/sf100/social_network/dynamic/person_knows_person_0_0.csv /root/dataset/sf100/person_knows_person 0 edge to_undirected
./Csv2Parquet-is3 /root/dataset/sf100/social_network/dynamic/person_0_0.csv /root/dataset/sf100/person_knows_person 0 vertex
# sf300
./Csv2Parquet-is3 /root/dataset/sf300/social_network/dynamic/person_knows_person_0_0.csv /root/dataset/sf300/person_knows_person 0 edge to_undirected
./Csv2Parquet-is3 /root/dataset/sf300/social_network/dynamic/person_0_0.csv /root/dataset/sf300/person_knows_person 0 vertex
# Generate the graphar dataset
./data-generator-label /root/dataset/sf30/social_network/dynamic/person_knows_person_0_0.csv /root/dataset/sf30/social_network/dynamic/person_0_0.csv /root/dataset/sf30/social_network/dynamic/person_0_0.csv person person csv csv /root/dataset/sf30/person_knows_person not-reverse to_undirected
./data-generator-label /root/dataset/sf100/social_network/dynamic/person_knows_person_0_0.csv /root/dataset/sf100/social_network/dynamic/person_0_0.csv /root/dataset/sf100/social_network/dynamic/person_0_0.csv person person csv csv /root/dataset/sf100/person_knows_person not-reverse to_undirected
./data-generator-label /root/dataset/sf300/social_network/dynamic/person_knows_person_0_0.csv /root/dataset/sf300/social_network/dynamic/person_0_0.csv /root/dataset/sf300/social_network/dynamic/person_0_0.csv person person csv csv /root/dataset/sf300/person_knows_person not-reverse to_undirected

# sf30
./Csv2Parquet /root/dataset/sf30/social_network/dynamic/post_0_0.csv /root/dataset/sf30/social_network/parquet/bi/post_0_0 0
./Csv2Parquet /root/dataset/sf30/social_network/dynamic/post_hasTag_tag_0_0.csv /root/dataset/sf30/social_network/parquet/bi/post_hasTag_tag_0_0 0
./Csv2Parquet /root/dataset/sf30/social_network/dynamic/comment_hasTag_tag_0_0.csv /root/dataset/sf30/social_network/parquet/bi/comment_hasTag_tag_0_0 0
# sf100
./Csv2Parquet /root/dataset/sf100/social_network/dynamic/post_0_0.csv /root/dataset/sf100/social_network/parquet/bi/post_0_0 0
./Csv2Parquet /root/dataset/sf100/social_network/dynamic/post_hasTag_tag_0_0.csv /root/dataset/sf100/social_network/parquet/bi/post_hasTag_tag_0_0 0
./Csv2Parquet /root/dataset/sf100/social_network/dynamic/comment_hasTag_tag_0_0.csv /root/dataset/sf100/social_network/parquet/bi/comment_hasTag_tag_0_0 0
# sf300
./Csv2Parquet /root/dataset/sf300/social_network/dynamic/post_0_0.csv /root/dataset/sf300/social_network/parquet/bi/post_0_0 0
./Csv2Parquet /root/dataset/sf300/social_network/dynamic/post_hasTag_tag_0_0.csv /root/dataset/sf300/social_network/parquet/bi/post_hasTag_tag_0_0 0
./Csv2Parquet /root/dataset/sf300/social_network/dynamic/comment_hasTag_tag_0_0.csv /root/dataset/sf300/social_network/parquet/bi/comment_hasTag_tag_0_0 0
