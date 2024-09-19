#!/bin/bash
# ./data-generator source_file outut_file_prefix vertex_num isDirected isWeighted isSorted isReversed delemiter ignoreLineNum
#
echo 'uk-2002'
./data-generator /mnt/ldbc/lixue/uk-2002/uk-2002.mtx /root/dataset/uk-2002 18520486 false true false false space 1
echo 'arabic-2005'
./data-generator /mnt/ldbc/lixue/arabic-2005/arabic-2005.mtx /root/dataset/arabic-2005 22744081 false true false false space 1
echo 'uk-2005'
./data-generator /mnt/ldbc/lixue/uk-2005/uk-2005.mtx /root/dataset/uk-2005 39459926 false true false false space 1
echo 'webbase-2001'
./data-generator /mnt/ldbc/lixue/webbase-2001/webbase-2001.mtx /root/dataset/webbase-2001 118142156 false true false false space 1