!/bin/bash

# Generate the base dataset
# ./release/Csv2Parquet-ldbc sourcefile output_file_prefix ignore_rows type [to_undirected]
#!/bin/bash
./release/Csv2Parquet-ldbc /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/post_0_0.csv /tmp/sf30/bi/post_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/post_hasTag_tag_0_0.csv /tmp/sf30/bi/post_hasTag_tag_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/comment_hasTag_tag_0_0.csv /tmp/sf30/bi/comment_hasTag_tag_0_0 0 edge
# sf100
./release/Csv2Parquet-ldbc /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/post_0_0.csv /tmp/sf100/bi/post_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/post_hasTag_tag_0_0.csv /tmp/sf100/bi/post_hasTag_tag_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/comment_hasTag_tag_0_0.csv /tmp/sf100/bi/comment_hasTag_tag_0_0 0 edge
# sf300
./release/Csv2Parquet-ldbc /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/post_0_0.csv /tmp/sf300/bi/post_0_0 0 vertex
./release/Csv2Parquet-ldbc /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/post_hasTag_tag_0_0.csv /tmp/sf300/bi/post_hasTag_tag_0_0 0 edge
./release/Csv2Parquet-ldbc /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/comment_hasTag_tag_0_0.csv /tmp/sf300/bi/comment_hasTag_tag_0_0 0 edge

# Generate the graphar dataset
# ./data-generator-label edge_source_file src_vertex_source_file dst_vertex_source_file src_label dst_label src_file_type dst_file_type output_file_prefix reverse/not-reverse to_undirected/not_to_undirected [just_vertex]
# sf30
# post_hasTag_tag_0_0
./release/data-generator-label /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/post_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/post_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf10/social_network/static/tag_0_0.csv post tag csv csv /tmp/sf30/bi/post_hasTag_tag_0_0_reverse reverse not_to_undirected
./release/data-generator-label /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/comment_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/comment_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf10/social_network/static/tag_0_0.csv comment tag csv csv /tmp/sf30/bi/comment_hasTag_tag_0_0_reverse reverse not_to_undirected
# sf100
./release/data-generator-label /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/post_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/post_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf10/social_network/static/tag_0_0.csv post tag csv csv /tmp/sf100/bi/post_hasTag_tag_0_0_reverse reverse not_to_undirected
./release/data-generator-label /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/comment_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/comment_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf10/social_network/static/tag_0_0.csv comment tag csv csv /tmp/sf100/bi/comment_hasTag_tag_0_0 not_reverse not_to_undirected

# sf300
./release/data-generator-label /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/post_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/post_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf10/social_network/static/tag_0_0.csv post tag csv csv /tmp/sf300/bi/post_hasTag_tag_0_0_reverse reverse not_to_undirected
./release/data-generator-label /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/comment_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/comment_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf10/social_network/static/tag_0_0.csv comment tag csv csv /tmp/sf300/bi/comment_hasTag_tag_0_0_reverse reverse not_to_undirected