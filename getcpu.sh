#/bin/bash

outputdir="/Users/liang/tmp/sdr_home/"
getavgpy="/Users/liang/tmp/py/getavg.py"

# $1 may contain dir path,
filename=$1
cp $filename $outputdir # copy original sdr to sdr_home

# build the output file name
outputfile_cpu=`echo $1 | awk -F"/" '{print($NF)}' | awk -F "tdw-" '{print($2)}' | awk -F "_" '{print($2"_"$1"_"$3"_cpu.txt")}'`
outputfile_vcore=`echo $1 | awk -F"/" '{print($NF)}' | awk -F "tdw-" '{print($2)}' | awk -F "_" '{print($2"_"$1"_"$3"_vcore.txt")}'`

# get cpu utilization
grep -A 1 avg-cpu $filename | grep -v avg-cpu | grep -v "\-\-" | awk -F " " '{print($1,$2,$3,$4,$5,$6)}' > $outputdir$outputfile_cpu

# get actual vcore
grep -A 1 mr_sparksql_scala_container_coun $filename | grep -v mr_sparksql_scala_container_coun | grep -v "\-\-" | awk -F" " '{print($9,$11,$13)}' | awk -F "\/" '{printf("%d %d %d %d %d %d\n",$1,$2,$3,$4,$5,$1+$3+$5)}' > $outputdir$outputfile_vcore

# calculate the avg and print
echo "|"
echo "|"
echo "==vcore:"$outputfile_vcore
python $getavgpy $outputdir$outputfile_vcore

echo "==cpu:  "$outputfile_cpu
python $getavgpy $outputdir$outputfile_cpu
