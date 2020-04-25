#!/bin/bash
cp pom-worker.xml pom.xml
mvn package
aws s3 cp target/Worker-1.0.jar s3://disthw1bucket/Worker-1.0.jar
cp pom-manager.xml pom.xml
mvn package
aws s3 cp target/Manager-1.0.jar s3://disthw1bucket/Manager-1.0.jar
cp pom-local.xml pom.xml
mvn package
java -jar target/Local-1.0.jar $1 $2 $3
