#!/bin/bash

export JAVA_OPTS="-Xms512m -Xmx6g"
java -jar -cp . -Dconfig.file=application.conf -Dlogback.configurationFile=application.xml -jar $*
