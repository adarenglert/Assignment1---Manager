#!/bin/bash
sudo apt-get install zip unzip
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws configure set aws_access_key_id AKIA3W3ZEDH6VT4MVK6E
aws configure set aws_secret_access_key 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
aws configure set default.region us-east-1
aws s3 cp s3://disthw1bucket/Manager-1.0.jar manager.jar
java -jar manager.jar disthw1bucket loc_man_key man_loc_key#0


gad account details
account: AKIA3W3ZEDH63YWE6323
secret: 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

ToImage	http://www.fairwaymarket.com/wp-content/uploads/2013/09/Kosher-Passover-Menu_FINAL.pdf