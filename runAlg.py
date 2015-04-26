#!/bin/python
# universal script for running different program

import os
import sys
from subprocess import call

algList = ["Apriori", "FPGrowth_itemsets"]
alg 	= "Apriori"
DB 	= "DB/retail.txt"
minSup 	= "0.01"

if (len(sys.argv) == 2):
    if (sys.argv[1] == "l"):
        print(algList)
    elif (sys.argv[1] == "h"):
    	print("usage: runAlg.py", "type", "algorithm", "DB", "minSup")

    # run python program
    if (sys.argv[1] == "py"):
        command = ["myAlg_PY/" + alg + ".py", DB, minSup]
        print(command)
        call(command)

    # run spmf program
    elif(sys.argv[1] == "spmf"):
        # java -jar spmf.jar run Apriori A1.txt output.txt 70%
        command = ["java", "-Xmx1024m", "-jar", "ref/spmf.jar",
                   "run", alg, DB, "output", minSup]
        print(command)
        call(command)

    # run spark program
    elif(sys.argv[1] == "spark"):
        # server = "spark://domba-02.cs.umanitoba.ca:7077"
        command = ["/usr/local/spark/bin/spark-submit",
                   "--class", "myAlg.spark." + alg.lower() + "." + alg,
                   "--master", "yarn",
                   "myAlg_spark/targets/" + alg + "-0.1.jar",
                   DB, minSup]
        print(command)
        call(command)

else:
    print("usage: runAlg.py", "type", "algorithm", "DB", "minSup")
