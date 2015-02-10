#!/bin/python

# import pprint


def loadDataset(filename):
    fp = open(filename, 'rt')

    rawData = fp.readlines()
    dbList = []

    for line in rawData:
        xact = line.split()
        xact = set(xact)
        dbList.append(xact)
    fp.close()

    # print("\n-->DATA SET:")
    # pprint.pprint(dbList)

    return dbList
