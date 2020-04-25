#!/bin/bash
#sudo apt-get install zip unzip
#curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
#unzip awscliv2.zip
#sudo ./aws/install
aws configure set aws_access_key_id AKIA3W3ZEDH6VT4MVK6E
aws configure set aws_secret_access_key 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
aws configure set default.region us-east-1
aws s3 cp s3://disthw1bucket/Manager.jar Manager.jar
java -jar