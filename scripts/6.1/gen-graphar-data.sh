#!/bin/bash
# ./data-generator source_file output_file_prefix vertex_num isDirected isWeighted isSorted isReversed delemiter ignoreLineNum
#
echo 'degme'
./data-generator /mnt/ldbc/lixue/degme/degme_full.mtx /root/dataset/degme 659416 false true false false space 2
echo 'cont1_l'
./data-generator /mnt/ldbc/lixue/cont1-l/cont1_l_full.mtx /root/dataset/cont1_l 1921597 false true false true space 2
echo 'wiki'
./data-generator /mnt/ldbc/lixue/LargeRegFile/LargeRegFile.mtx /root/dataset/LargeRegFile 2111155 false true false true space 2
echo 'tp-6'
./data-generator /mnt/ldbc/lixue/tp-6/tp-6.mtx /root/dataset/tp-6 1100000 true true false false space 0
echo 'hollywood-2009'
./data-generator /mnt/ldbc/lixue/hollywood-2009/hollywood-2009.mtx /root/dataset/hollywood-2009 1139906 false true false false space 1
echo 'orkut'
./data-generator /mnt/ldbc/lixue/orkut/com-orkut.ungraph.txt /root/dataset/orkut 3072442 true false false false tab 0
echo 'indochina-2004'
./data-generator /mnt/ldbc/lixue/indochina-2004/indochina-2004.mtx /root/dataset/indochina-2004 7414867 false true false false space 1
echo 'wiki'
./data-generator /mnt/ldbc/lixue/wiki/wikipedia_link_en/out.wikipedia_link_en /root/dataset/wiki 13593032 false false false true tab 1
echo 'uk-2005'
./data-generator /mnt/ldbc/lixue/uk-2005/uk-2005.mtx /root/dataset/uk-2005 39459926 false true false false space 1
