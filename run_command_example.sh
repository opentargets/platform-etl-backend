#!/bin/bash
path=$(pwd)
/usr/lib/jvm/java-8-openjdk-amd64/bin/java -server -Xms1G -Xmx6G -Xss1M -XX:+CMSClassUnloadingEnabled -Dlogback.configurationFile=application.xml -Dconfig.file=./application.conf -classpath . -jar $*