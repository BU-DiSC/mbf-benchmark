#!/bin/bash

DISTRIBUTIONs=("zipfian" "uniform" "norm-dev-5")
Z_LIST=("0.0" "0.5" "1.0")

WORKLOAD_DIR="/scratchHDDa/jmun/workloads/mbf/K32/"

MEM=8388608

for dis in ${DISTRIBUTIONs[@]}
do
	for Z in ${Z_LIST[@]}
	do
		pathname="${WORKLOAD_DIR}${dis}/Z${Z}_${dis}_workload.txt"
		output_pathname="Z${Z}_${dis}_alpha.txt"
		./mbf-benchmark -T 4 -E 64 --dr --wp=${pathname} -P 1024 -b 10 --blk_cache --mod --Pb 5 --BCC=${mem} --WQ 3000 2>&1 | tee ${output_pathname}
	done
done

