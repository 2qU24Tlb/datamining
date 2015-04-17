#!/bin/bash

app=$1
cmd="/usr/local/spark/bin/spark-submit "
input="hdfs:///data/A1.txt"
output=""

echo "run:"
${cmd} --class myAlg.spark.${1,,}.${app} --master yarn ${app}/target/${app}-0.1.jar ${input} ${output}
