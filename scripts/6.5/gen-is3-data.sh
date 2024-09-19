!/bin/bash

# Generate the base dataset
# ./release/Csv2Parquet-is3 sourcefile output_file_prefix ignore_rows type [to_undirected]
./release/Csv2Parquet-is3 /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/person_knows_person_0_0.csv /tmp/sf30/parquet/person_knows_person 0 edge to_undirected
./release/Csv2Parquet-is3 /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/person_0_0.csv /tmp/sf30/person_knows_person 0 vertex
# sf100
./release/Csv2Parquet-is3 /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/person_knows_person_0_0.csv /tmp/sf100/person_knows_person 0 edge to_undirected
./release/Csv2Parquet-is3 /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/person_0_0.csv /tmp/sf100/person_knows_person 0 vertex
# sf300
./release/Csv2Parquet-is3 /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/person_knows_person_0_0.csv /tmp/sf100/person_knows_person 0 edge to_undirected
./release/Csv2Parquet-is3 /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/person_0_0.csv /tmp/sf100/person_knows_person 0 vertex

# Generate the graphar dataset
# ./data-generator-label edge_source_file src_vertex_source_file dst_vertex_source_file src_label dst_label src_file_type dst_file_type output_file_prefix reverse/not-reverse to_undirected/not_to_undirected
./release/data-generator-label /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/person_knows_person_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/person_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/person_0_0.csv person person csv csv /tmp/sf30/person_knows_person not-reverse to_undirected
./release/data-generator-label /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/person_knows_person_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/person_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/person_0_0.csv person person csv csv /tmp/sf100/person_knows_person not-reverse to_undirected
./release/data-generator-label /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/person_knows_person_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/person_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/person_0_0.csv person person csv csv /tmp/sf300/person_knows_person not-reverse to_undirected
