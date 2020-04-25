#!/bin/bash
cp pom-worker.xml pom.xml
mvn package
aws s3 cp target/Worker-1.0.jar s3://disthw1bucket/Worker.jar
