#!/bin/bash

if [ $1 == "com" ]; then
    scalac SVTDriver.scala
elif [ $1 == "run" ]; then
    scala -Dscala.time myAlg.spark.svt.MyTest /home/hao/Develop/projects/datamining/DB/retail.txt 0.5
fi