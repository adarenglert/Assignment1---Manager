#!/bin/bash
aws s3 rm s3://disthw1results/ --recursive --exclude "publicprefix/*"
aws s3 rm s3://disthw1bucket/ --recursive --exclude "*.jar"
queues=$(aws sqs list-queues)
for q in ${queues[@]}
do
	if [ "${q:0:1}" == "\"" ]; then
			x=${q:1:${#q}-2}
		if [ $x != "QueueUrls\"" ]; then
			if [ "${q: -1}" != "\"" ]; then
				y=${x:0:${#x}-1}
				if [ "${q: -2}" != "g" ]; then
					aws sqs delete-queue --queue-url $y
				fi
			else
				aws sqs delete-queue --queue-url $x
			fi
		fi
	fi
done

