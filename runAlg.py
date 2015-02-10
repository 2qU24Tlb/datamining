#!/bin/python

import os
import sys
from subprocess import call

#java -jar spmf.jar run Apriori A1.txt output.txt 70%

algList = ["Apriori"]

alg = ""
db = ""
minsup = 0.5

if (len(sys.argv) == 5):
    alg = sys.argv[2]
    db = sys.argv[3]
    minsup = sys.argv[4]

    if (sys.argv[1] == "my"): 
        print(["myAlg_PY/" + alg + ".py", db, minsup])
        call(["myAlg_PY/" + alg + ".py", db, minsup])
    elif(sys.argv[1] == "spmf"):
        #print(["java", "-jar", "spmf/spmf.jar", \
        #       "run", alg, db, "output", minsup])
        call(["java", "-jar", "spmf/spmf.jar", \
              "run", alg, db, "output", minsup])

elif (len(sys.argv) == 2 and sys.argv[1] == "list"):
    print(algList)
elif (len(sys.argv) == 2 and sys.argv[1] == "exam"):
    print("./runAlg.py my Apriori DB/A1.txt 0.5")
else:
    print("runAlg.py", "algorithm", "DB", "minsup")
