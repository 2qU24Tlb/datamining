#!/bin/bash

app=$2
rcmd="/usr/local/spark-1.3.1-bin-hadoop2.6/bin/spark-submit "
pcmd="mvn package"
address="spark://domba-02.cs.umanitoba.ca:7077"
db1="/tmp/hao/100000.txt"
db2="/tmp/hao/mushrooms.txt"
minsup="0.01"
#output="hdfs:///output/results"

if [ $1 == "r" ]; then
    echo "run:"
    ${rcmd} --class myAlg.spark.${app,,}.${app} --master ${address} --executor-memory 20G ${app}/target/${app}-0.1.jar ${db1} ${minsup} 
elif [ $1 == "c" ]; then
    echo "compile:"
    cd ${app}
    ${pcmd}
elif [ $1 == "e" ]; then
    ${rcmd} --class org.apache.spark.examples.mllib.FPGrowthExample --master ${address}  --executor-memory 20G /usr/local/spark-1.3.1-bin-hadoop2.6/lib/spark-examples-1.3.1-hadoop2.6.0.jar --minSupport ${minsup} --numPartition 100 ${db2}
fi
