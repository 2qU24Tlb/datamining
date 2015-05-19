#!/bin/bash

APP=$2
RCMD="/usr/local/spark-1.3.1-bin-hadoop2.6/bin/spark-submit "
PCMD="mvn package"
ADDRESS="spark://domba-02.cs.umanitoba.ca:7077"
DB1="/tmp/hao/DB/retail.txt"
DB2="/tmp/hao/DB/retail.txt"
MINSUP="0.5"

if [ $1 == "run" ]; then
    ${RCMD} --class myAlg.spark.${APP,,}.${APP} --master ${ADDRESS} --executor-memory 10G ${APP}/target/${APP}-0.1.jar ${DB1} ${MINSUP} 
elif [ $1 == "com" ]; then
    cd ${APP}
    ${PCMD}
elif [ $1 == "exam" ]; then
    ${rcmd} --class org.apache.spark.examples.mllib.FPGrowthExample --master ${ADDRESS}  --executor-memory 10G /usr/local/spark-1.3.1-bin-hadoop2.6/lib/spark-examples-1.3.1-hadoop2.6.0.jar --minSupport ${MINSUP} --numPartition 100 ${DB2}
fi
