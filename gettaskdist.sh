#!/bin/bash

taskinfofile=$1

range=(0 10 20 30 40 50 60 70 80 90 100 110 120 300 600 1800 3600 7200 14400 144000) # 14400=4H
for (( i=0; i<${#range[@]}; i++ ))
do
	[ ${range[$i]} -eq 0 ] && continue
	j=$((i-1))
	echo -n ${range[$j]}" "${range[$i]}" " | tee -a tmpresult

	rm -rf tmptask
	cat $taskinfofile  | awk -F "," '{print($4)}' | sort -g | uniq -c | sed '1d' | while read line
	do
		tasktime=`echo $line | cut -d" " -f2`
		[ $tasktime -ge ${range[$j]} ] && [ $tasktime -lt ${range[$i]} ]  && echo $line >> tmptask
	done

	if [ -f tmptask ] 
	then
		# if just one line in tmptask, getsum.py doesn't work, so print the only line out directly
		[ `cat tmptask | wc -l` -eq "1" ] && cat tmptask | awk '{print($1)}' | tee -a  tmpresult && continue 
		python getsum.py tmptask | awk '{print($2)}' | sed -e "s/\.$//g" |  tee -a tmpresult && continue
	fi
	#if goes here, means there's no sampling in current range, put a 0 here 
	echo "0" | tee -a tmpresult
#	echo "----"
done

# put a sum line
echo "--"
#cat tmpresult
echo -n "SUM:"
sum=`python getsum.py tmpresult | sed -e "s/\s//g" | cut -d. -f3`
echo $sum
echo "--"

# calculate the percentage of each line
percentage_accumulated=0
cat tmpresult | while read line
do
	echo -n $line | sed -e "s/ /, /g"
	nbr=`echo $line | cut -d" " -f3`
	percentage=$(echo "scale=4;$nbr/$sum"|bc)
	percentage_accumulated=$(echo "scale=4;$percentage_accumulated+$percentage"|bc)
	#echo " "$percentage" "$percentage_accumulated
	printf ", %.2f%%, %.2f%%\n" $(echo "$percentage*100"|bc) $(echo "$percentage_accumulated*100"|bc)
done

rm -rf tmptask
rm -rf tmpresult

# add a line to see how git diff works
