#!/bin/bash

APP=$2
RCMD="/usr/local/spark/bin/spark-submit "
ADDRESS="spark://130.179.30.53:7077"
DB1="/import/helium-share/csgrad/zhangh15/DB/webdocs_lined.dat"
DB2="/import/helium-share/csgrad/zhangh15/DB/webdocs.dat"
MINSUP="0.5"
CONF="--executor-memory 20G --conf spark.eventLog.enabled=true"

if [ $1 == "run" ]; then
    ${RCMD} --class myAlg.spark.${APP,,}.MyTest --master ${ADDRESS} ${CONF} ${APP}/target/${APP}-0.1.jar ${DB1} ${MINSUP} 
elif [ $1 == "com" ]; then
    cd ${APP}
    mvn clean -q && mvn package -q
elif [ $1 == "exam" ]; then
    ${RCMD} --class org.apache.spark.examples.mllib.FPGrowthExample --master ${ADDRESS}  ${CONF} /usr/local/spark/lib/spark-examples-1.4.0-hadoop2.6.0.jar --minSupport ${MINSUP} --numPartition 4 ${DB2}
fi
