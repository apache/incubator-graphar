!/bin/bash

# Generate the base dataset
# ./release/Csv2Parquet-ldbc sourcefile output_file_prefix ignore_rows type [to_undirected]
./release/Csv2Parquet-ldbc /mnt/ldbc/sf30/social_network/dynamic/person_knows_person_0_0.csv /root/dataset/sf30/person_knows_person 0 edge to_undirected
./release/Csv2Parquet-ldbc /mnt/ldbc/sf30/social_network/dynamic/person_0_0.csv /root/dataset/sf30/person_knows_person 0 vertex
# sf100
./release/Csv2Parquet-ldbc /mnt/ldbc/sf100/social_network/dynamic/person_knows_person_0_0.csv /root/dataset/sf100/person_knows_person 0 edge to_undirected
./release/Csv2Parquet-ldbc /mnt/ldbc/sf100/social_network/dynamic/person_0_0.csv /root/dataset/sf100/person_knows_person 0 vertex
# sf300
./release/Csv2Parquet-ldbc /mnt/ldbc/sf300/social_network/dynamic/person_knows_person_0_0.csv /root/dataset/sf100/person_knows_person 0 edge to_undirected
./release/Csv2Parquet-ldbc /mnt/ldbc/sf300/social_network/dynamic/person_0_0.csv /root/dataset/sf100/person_knows_person 0 vertex

# Generate the graphar dataset
# ./data-generator-label edge_source_file src_vertex_source_file dst_vertex_source_file src_label dst_label src_file_type dst_file_type output_file_prefix reverse/not-reverse to_undirected/not_to_undirected
./release/data-generator-label /mnt/ldbc/sf30/social_network/dynamic/person_knows_person_0_0.csv /mnt/ldbc/sf30/social_network/dynamic/person_0_0.csv /mnt/ldbc/sf30/social_network/dynamic/person_0_0.csv person person csv csv /root/dataset/sf30/person_knows_person not-reverse to_undirected
./release/data-generator-label /mnt/ldbc/sf100/social_network/dynamic/person_knows_person_0_0.csv /mnt/ldbc/sf100/social_network/dynamic/person_0_0.csv /mnt/ldbc/sf100/social_network/dynamic/person_0_0.csv person person csv csv /root/dataset/sf100/person_knows_person not-reverse to_undirected
./release/data-generator-label /mnt/ldbc/sf300/social_network/dynamic/person_knows_person_0_0.csv /mnt/ldbc/sf300/social_network/dynamic/person_0_0.csv /mnt/ldbc/sf300/social_network/dynamic/person_0_0.csv person person csv csv /root/dataset/sf300/person_knows_person not-reverse to_undirected

# Generate the base dataset
# ./release/Csv2Parquet-ldbc sourcefile output_file_prefix ignore_rows type [to_undirected]
#!/bin/bash
./release/Csv2Parquet-ldbc /mnt/ldbc/sf30/social_network/dynamic/person_0_0.csv /root/dataset/sf30/person_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/sf30/social_network/dynamic/comment_0_0.csv /root/dataset/sf30/comment_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/sf30/social_network/dynamic/post_hasCreator_person_0_0.csv /root/dataset/sf30/post_hasCreator_person_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf30/social_network/dynamic/comment_hasCreator_person_0_0.csv /root/dataset/sf30/comment_hasCreator_person_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf30/social_network/dynamic/comment_replyOf_post_0_0.csv /root/dataset/sf30/comment_replyOf_post_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf30/social_network/dynamic/comment_replyOf_comment_0_0.csv /root/dataset/sf30/comment_replyOf_comment_0_0 0 edge
# sf100
./release/Csv2Parquet-ldbc /mnt/ldbc/sf100/social_network/dynamic/person_0_0.csv /root/dataset/sf100/person_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/sf100/social_network/dynamic/comment_0_0.csv /root/dataset/sf100/comment_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/sf100/social_network/dynamic/post_hasCreator_person_0_0.csv /root/dataset/sf100/post_hasCreator_person_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf100/social_network/dynamic/comment_hasCreator_person_0_0.csv /root/dataset/sf100/comment_hasCreator_person_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf100/social_network/dynamic/comment_replyOf_post_0_0.csv /root/dataset/sf100/comment_replyOf_post_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf100/social_network/dynamic/comment_replyOf_comment_0_0.csv /root/dataset/sf100/comment_replyOf_comment_0_0 0 edge
# sf300
./release/Csv2Parquet-ldbc /mnt/ldbc/sf300/social_network/dynamic/person_0_0.csv /root/dataset/sf300/person_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/sf300/social_network/dynamic/comment_0_0.csv /root/dataset/sf300/comment_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/sf300/social_network/dynamic/post_hasCreator_person_0_0.csv /root/dataset/sf300/post_hasCreator_person_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf300/social_network/dynamic/comment_hasCreator_person_0_0.csv /root/dataset/sf300/comment_hasCreator_person_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf300/social_network/dynamic/comment_replyOf_post_0_0.csv /root/dataset/sf300/comment_replyOf_post_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf300/social_network/dynamic/comment_replyOf_comment_0_0.csv /root/dataset/sf300/comment_replyOf_comment_0_0 0 edge

# Generate the graphar dataset
# ./data-generator-label edge_source_file src_vertex_source_file dst_vertex_source_file src_label dst_label src_file_type dst_file_type output_file_prefix reverse/not-reverse to_undirected/not_to_undirected [just_vertex]
# sf30
# generate person_0_0_index.sf300
./release/data-generator-label /mnt/ldbc/sf30/social_network/dynamic/comment_hasCreator_person_0_0.csv  /mnt/ldbc/sf30/social_network/dynamic/comment_0_0.csv /mnt/ldbc/sf30/social_network/dynamic/person_0_0.csv comment person csv csv /root/dataset/sf30/person_0_0_index.parquet not_to_undirected just_vertex
# generate comment_0_0_index.sf300
./release/data-generator-label /mnt/ldbc/sf30/social_network/dynamic/comment_hasCreator_person_0_0.csv  /mnt/ldbc/sf30/social_network/dynamic/comment_0_0.csv /mnt/ldbc/sf30/social_network/dynamic/person_0_0.csv comment person csv csv /root/dataset/sf30/comment_0_0_index.parquet not_to_undirected just_vertex
# post_hasCreator_person_0_0
./release/data-generator-label /mnt/ldbc/sf30/social_network/dynamic/post_hasCreator_person_0_0.csv /mnt/ldbc/sf30/social_network/dynamic/post_0_0.csv /mnt/ldbc/sf30/social_network/dynamic/person_0_0.csv post person csv csv /root/dataset/sf30/post_hasCreator_person_0_0 reverse not_to_undirected
# commet_hasCreator_person_0_0 reverse
./release/data-generator-label /mnt/ldbc/sf30/social_network/dynamic/comment_hasCreator_person_0_0.csv /mnt/ldbc/sf30/social_network/comment_0_0_index.parquet /mnt/ldbc/sf30/social_network/dynamic/person_0_0.csv comment person parquet csv /root/dataset/sf30/comment_hasCreator_person_0_0 reverse not_to_undirected
# commet_hasCreator_person_0_0 not reverse
./release/data-generator-label /mnt/ldbc/sf30/social_network/dynamic/comment_hasCreator_person_0_0.csv /mnt/ldbc/sf30/social_network/comment_0_0_index.parquet /mnt/ldbc/sf30/social_network/dynamic/person_0_0.csv comment person parquet csv /root/dataset/sf30/comment_hasCreator_person_0_0_alter not-reverse not_to_undirected
# comment_replyOf_post_0_0
./release/data-generator-label /mnt/ldbc/sf30/social_network/dynamic/comment_replyOf_post_0_0.csv /mnt/ldbc/sf30/social_network/comment_0_0_index.parquet /mnt/ldbc/sf30/social_network/dynamic/post_0_0.csv comment post parquet csv /root/dataset/sf30/comment_replyOf_post_0_0 reverse not_to_undirected
# comment_replyOf_comment_0_0
./release/data-generator-label /mnt/ldbc/sf30/social_network/dynamic/comment_replyOf_comment_0_0.csv /mnt/ldbc/sf30/social_network/comment_0_0_index.parquet /mnt/ldbc/sf30/social_network/comment_0_0_index.parquet comment comment parquet parquet  /root/dataset/sf30/comment_replyOf_comment_0_0 reverse not_to_undirected

# Generate the base dataset
# ./release/Csv2Parquet-ldbc sourcefile output_file_prefix ignore_rows type [to_undirected]
#!/bin/bash
./release/Csv2Parquet-ldbc /mnt/ldbc/sf30/social_network/dynamic/post_0_0.csv /root/dataset/sf30/bi/post_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/sf30/social_network/dynamic/post_hasTag_tag_0_0.csv /root/dataset/sf30/bi/post_hasTag_tag_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf30/social_network/dynamic/comment_hasTag_tag_0_0.csv /root/dataset/sf30/bi/comment_hasTag_tag_0_0 0 edge
# sf100
./release/Csv2Parquet-ldbc /mnt/ldbc/sf100/social_network/dynamic/post_0_0.csv /root/dataset/sf100/bi/post_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/sf100/social_network/dynamic/post_hasTag_tag_0_0.csv /root/dataset/sf100/bi/post_hasTag_tag_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf100/social_network/dynamic/comment_hasTag_tag_0_0.csv /root/dataset/sf100/bi/comment_hasTag_tag_0_0 0 edge
# sf300
./release/Csv2Parquet-ldbc /mnt/ldbc/sf300/social_network/dynamic/post_0_0.csv /root/dataset/sf300/bi/post_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/sf300/social_network/dynamic/post_hasTag_tag_0_0.csv /root/dataset/sf300/bi/post_hasTag_tag_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/sf300/social_network/dynamic/comment_hasTag_tag_0_0.csv /root/dataset/sf300/bi/comment_hasTag_tag_0_0 0 edge

# Generate the graphar dataset
# ./data-generator-label edge_source_file src_vertex_source_file dst_vertex_source_file src_label dst_label src_file_type dst_file_type output_file_prefix reverse/not-reverse to_undirected/not_to_undirected [just_vertex]
# sf30
# post_hasTag_tag_0_0
./release/data-generator-label /mnt/ldbc/sf30/social_network/dynamic/post_hasTag_tag_0_0.csv /mnt/ldbc/sf30/social_network/dynamic/post_0_0.csv /mnt/ldbc/sf10/social_network/static/tag_0_0.csv post tag csv csv /root/dataset/sf30/bi/post_hasTag_tag_0_0_reverse reverse not_to_undirected
./release/data-generator-label /mnt/ldbc/sf30/social_network/dynamic/comment_hasTag_tag_0_0.csv /mnt/ldbc/sf30/social_network/dynamic/comment_0_0.csv /mnt/ldbc/sf10/social_network/static/tag_0_0.csv comment tag csv csv /root/dataset/sf30/bi/comment_hasTag_tag_0_0_reverse reverse not_to_undirected
# sf100
./release/data-generator-label /mnt/ldbc/sf100/social_network/dynamic/post_hasTag_tag_0_0.csv /mnt/ldbc/sf100/social_network/dynamic/post_0_0.csv /mnt/ldbc/sf10/social_network/static/tag_0_0.csv post tag csv csv /root/dataset/sf100/bi/post_hasTag_tag_0_0_reverse reverse not_to_undirected
./release/data-generator-label /mnt/ldbc/sf100/social_network/dynamic/comment_hasTag_tag_0_0.csv /mnt/ldbc/sf100/social_network/dynamic/comment_0_0.csv /mnt/ldbc/sf10/social_network/static/tag_0_0.csv comment tag csv csv /root/dataset/sf100/bi/comment_hasTag_tag_0_0 not_reverse not_to_undirected

# sf300
./release/data-generator-label /mnt/ldbc/sf300/social_network/dynamic/post_hasTag_tag_0_0.csv /mnt/ldbc/sf300/social_network/dynamic/post_0_0.csv /mnt/ldbc/sf10/social_network/static/tag_0_0.csv post tag csv csv /root/dataset/sf300/bi/post_hasTag_tag_0_0_reverse reverse not_to_undirected
./release/data-generator-label /mnt/ldbc/sf300/social_network/dynamic/comment_hasTag_tag_0_0.csv /mnt/ldbc/sf300/social_network/dynamic/comment_0_0.csv /mnt/ldbc/sf10/social_network/static/tag_0_0.csv comment tag csv csv /root/dataset/sf300/bi/comment_hasTag_tag_0_0_reverse reverse not_to_undirected