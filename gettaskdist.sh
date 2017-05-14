#!/bin/bash

taskinfofile=$1

range=(0 10 20 30 40 50 60 70 80 90 100 110 120 300 600 1800 3600 7200 14400 144000) # 14400=4H
for (( i=0; i<${#range[@]}; i++ ))
do
	[ ${range[$i]} -eq 0 ] && continue
	j=$((i-1))
	echo -n ${range[$j]}" - "${range[$i]}" "

	rm -rf tmptask
	cat $taskinfofile  | awk -F "," '{print($4)}' | sort -g | uniq -c | sed '1d' | while read line
	do
		tasktime=`echo $line | cut -d" " -f2`
		[ $tasktime -ge ${range[$j]} ] && [ $tasktime -lt ${range[$i]} ]  && echo $line >> tmptask
	done

	if [ -f tmptask ] 
	then
		# if just one line in tmptask, getsum doesn't work, so print the only line out directly
		[ `cat tmptask | wc -l` -eq "1" ] && sed -e 's/^/\[ /' -e 's/$/ \]/' tmptask && continue 
		python getsum.py tmptask && continue
	fi
	echo
#	echo "----"
done


rm -rf tmptask

# add a line to see how git diff works
