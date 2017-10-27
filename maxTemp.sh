#!/bin/bash
echo "Year"  "Max"     "  Min"

cd /home/jay/Eclipse/workspace/hadoop/input
for file in ./*
do
	
	name=$(basename $file)
	echo -e ${name%%.*} "\c"
       awk '{temp=substr($0,88,5)+0; quality=substr($0,93,1); if(temp!=9999 && quality~/[01459]/ && temp>max) max=temp; if(temp!=9999 && quality~/[01459]/ && temp<min) min=temp;}END{print max,'\t',min}' $name

done

