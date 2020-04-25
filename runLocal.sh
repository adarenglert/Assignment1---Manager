#!/bin/bash
cp pom-local.xml pom.xml
mvn package
java -jar target/Local-1.0.jar $1 $2 $3 > log.txt
xdg-open $2.html
