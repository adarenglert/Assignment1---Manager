#!/bin/bash
cp pom-manager.xml pom.xml
mvn package
aws s3 cp target/Manager-1.0.jar s3://disthw1bucket/Manager.jar
