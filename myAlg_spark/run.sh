#!/bin/bash

app=$2
rcmd="/usr/local/spark/bin/spark-submit "
pcmd="mvn package"
#input="hdfs:///data/A1.txt"
#output="hdfs:///output/results"

if [ $1 == "run" ]; then
    echo "run:"
    #/usr/local/hadoop/bin/hadoop fs -rm -r /output/*
    #${rcmd} --class myAlg.spark.${app,,}.${app} --master yarn ${app}/target/${app}-0.1.jar ${input} ${output}
    ${rcmd} --class myAlg.spark.${app,,}.${app} --master spark://myArch:7077 ${app}/target/${app}-0.1.jar ${input} ${output}
elif [ $1 == "com" ]; then
    echo "compile:"
    cd ${app}
    ${pcmd}
fi
