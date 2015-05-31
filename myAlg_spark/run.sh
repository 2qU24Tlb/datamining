#!/bin/bash

APP=$2
RCMD="/usr/local/spark/bin/spark-submit "
ADDRESS="spark://domba-02.cs.umanitoba.ca:7077"
DB1="/tmp/hao/DB/retail.txt"
DB2="/tmp/hao/DB/retail_line.txt"
MINSUP="0.1"
CONF="--executor-memory 10G --conf spark.eventLog.enabled=true"

if [ $1 == "run" ]; then
    ${RCMD} --class myAlg.spark.${APP,,}.MyTest --master ${ADDRESS} ${CONF} ${APP}/target/${APP}-0.1.jar ${DB1} ${MINSUP} 
elif [ $1 == "com" ]; then
    cd ${APP}
    mvn clean && mvn package
elif [ $1 == "exam" ]; then
    ${RCMD} --class org.apache.spark.examples.mllib.FPGrowthExample --master ${ADDRESS}  ${CONF} /usr/local/spark/lib/spark-examples-1.3.1-hadoop2.6.0.jar --minSupport ${MINSUP} --numPartition 4 ${DB2}
fi
