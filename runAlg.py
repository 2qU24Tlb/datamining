#!/bin/python
# universal script for running different program

import os
import sys
from subprocess import call

algList = ["Apriori"]
alg = ""
db = ""
minsup = 0.5

if (len(sys.argv) == 2):
    if (sys.argv[1] == "list"):
        print(algList)
    elif (sys.argv[1] == "example"):
        print("./runAlg.py py Apriori DB/A1.txt 0.5")

elif (len(sys.argv) == 5):
    alg = sys.argv[2]
    db = sys.argv[3]
    minsup = sys.argv[4]

    # run python program
    if (sys.argv[1] == "py"):
        command = ["myAlg_PY/" + alg + ".py", db, minsup]
        print(command)
        call(command)

    # run spmf program
    elif(sys.argv[1] == "spmf"):
        # java -jar spmf.jar run Apriori A1.txt output.txt 70%
        command = ["java", "-jar", "ref/spmf.jar",
                   "run", alg, db, "output", minsup]
        print(command)
        call(command)

    # run spark program
    elif(sys.argv[1] == "spark"):
        # server = "spark://myArch:7077"
        command = ["/usr/local/spark/bin/spark-submit",
                   "--class", "myAlg.spark." + alg.lower() + "." + alg,
                   "--master", "yarn",
                   "myAlg_spark/targets/" + alg + "-0.1.jar",
                   db, minsup]
        print(command)
        call(command)

else:
    print("usage: runAlg.py", "type", "algorithm", "DB", "minsup")
