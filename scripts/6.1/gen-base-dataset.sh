!/bin/bash

# Generate the base dataset
./release/Csv2Parquet /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/post_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf30/social_network/parquet/bi/post_0_0 0
./release/Csv2Parquet /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/post_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf30/social_network/parquet/bi/post_hasTag_tag_0_0 0
./release/Csv2Parquet /mnt/ldbc/mengke.mk/datagen/sf30/social_network/dynamic/comment_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf30/social_network/parquet/bi/comment_hasTag_tag_0_0 0
# sf100
./release/Csv2Parquet /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/post_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf100/social_network/parquet/bi/post_0_0 0
./release/Csv2Parquet /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/post_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf100/social_network/parquet/bi/post_hasTag_tag_0_0 0
./release/Csv2Parquet /mnt/ldbc/mengke.mk/datagen/sf100/social_network/dynamic/comment_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf100/social_network/parquet/bi/comment_hasTag_tag_0_0 0
# sf300
./release/Csv2Parquet /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/post_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf300/social_network/parquet/bi/post_0_0 0
./release/Csv2Parquet /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/post_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf300/social_network/parquet/bi/post_hasTag_tag_0_0 0
./release/Csv2Parquet /mnt/ldbc/mengke.mk/datagen/sf300/social_network/dynamic/comment_hasTag_tag_0_0.csv /mnt/ldbc/mengke.mk/datagen/sf300/social_network/parquet/bi/comment_hasTag_tag_0_0 0
