#/bin/bash

getavgpy="/mactmp/leonsws/leonsgithub/play/getavg.py"

# $1 may contain dir path,
filename=$1

# build the output file name
outputfile_cpu=`echo $1 | awk -F"/" '{print($NF)}' | awk -F "tdw-" '{print($2)}' | awk -F "_" '{print($2"_"$1"_"$3"_cpu.txt")}'`
outputfile_vcore=`echo $1 | awk -F"/" '{print($NF)}' | awk -F "tdw-" '{print($2)}' | awk -F "_" '{print($2"_"$1"_"$3"_vcore.txt")}'`
machineip=`echo $1 | awk -F"/" '{print($NF)}' | awk -F "tdw-" '{print($2)}' | awk -F "_" '{print($1)}'`

# get cpu utilization
grep -A 1 avg-cpu $filename | grep -v avg-cpu | grep -v "\-\-" | awk -F " " '{print($1,$2,$3,$4,$5,$6)}' > $outputfile_cpu

# get actual vcore
grep -A 1 mr_sparksql_scala_container_coun $filename | grep -v mr_sparksql_scala_container_coun | grep -v "\-\-" | awk -F" " '{print($9,$11,$13)}' | awk -F "/" '{printf("%d %d %d %d %d %d\n",$1,$2,$3,$4,$5,$1+$3+$5)}' > $outputfile_vcore

# calculate the avg and print
echo "===="
echo $machineip
echo -n "vcore:"
vcore=`python $getavgpy $outputdir$outputfile_vcore`
echo $vcore | sed -e "s/\[ //g" -e "s/\]//g"

echo -n "cpu:  "
cpu=`python $getavgpy $outputdir$outputfile_cpu`
echo $cpu |  sed -e "s/\[ //g" -e "s/\]//g"

TotalVcore=`echo $vcore | sed -e "s/\[ //g" -e "s/\]//g" | awk '{print($NF)}'`
user=`echo $cpu |  sed -e "s/\[ //g" -e "s/\]//g" | awk '{print($1)}'`
sys=`echo $cpu |  sed -e "s/\[ //g" -e "s/\]//g" | awk '{print($3)}'`
idle=`echo $cpu |  sed -e "s/\[ //g" -e "s/\]//g" | awk '{print($6)}'`
echo -e "vcore\tuser%\tsys%\tidle%"
echo -e $TotalVcore",\t"$user"%,\t"$sys"%,\t"$idle"%"

rm -rf $outputfile_vcore $outputfile_cpu
