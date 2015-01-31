#!/bin/python
import sys

def loadDataset(filename):
    fp = open(filename, 'rt')

    rawData = fp.readlines()
    dbList = []

    for line in rawData:
        xact = line.split()
        xact = set(xact)
        dbList.append(xact)
   
    fp.close()
    return dbList 
