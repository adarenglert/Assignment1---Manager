#!/bin/bash
aws s3api list-objects --bucket disthw1bucket --query 'Contents[].{Key: Key, Size: Size}'
aws s3api list-objects --bucket disthw1results --query 'Contents[].{Key: Key, Size: Size}'
queues=$(aws sqs list-queues)
for q in ${queues[@]}
do
	if [ "${q:0:1}" == "\"" ]; then
			x=${q:1:${#q}-2}
		if [ $x != "QueueUrls\"" ]; then
			if [ "${q: -1}" != "\"" ]; then
				y=${x:0:${#x}-1}
				echo $y
			else
				echo $x
			fi
		fi
	fi
done

