#!/bin/bash

ls -1 | while read line
do
	[ ! -d $line ] && continue

	cd $line
	[ `ls -1 | wc -l` -ne 1 ] && continue

	subdir=`ls -1`
	cd $subdir
	mv * ../
	cd ..
	rm -rf $subdir
	cd ..

done
