#!/bin/bash

homedir=$1

rm -rf tmpcpu.txt tmppower.txt
find $homedir | grep sdr | while read line
do
	./getcpu2.sh $line >> tmpcpu.txt
	machineip=`echo $line | awk -F"/" '{print($NF)}' | awk -F "tdw-" '{print($2)}' | awk -F "_" '{print($1)}' | sed -e "s/\-/\./g"`
	grep "Total Power" $line | awk '{print $10}' | sort | uniq -c | tail -5 | while read line
	do
		echo -n -e $machineip" " >> tmppower.txt
		echo $line >> tmppower.txt
	done
done

python quickview2.py $homedir 24

#rm -rf tmpcpu.txt
