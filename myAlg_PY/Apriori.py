#!/bin/python

# Modified from:
# https://github.com/cse40647/cse40647/blob/sp.14/10%20-%20Apriori.ipynb

# import pprint
import sys
import time
from loadDataset import loadDataset
from writeOutput import writeOutput


def createCandidates(dataset):
    c1 = []

    for transaction in dataset:
        for item in transaction:
            if [item] not in c1:
                c1.append([item])
    c1.sort()

    return c1


def joinCandiates(candidates):
    newCands = []
    k = len(candidates[0])
    length = len(candidates)

    for i in range(length):
        for j in range(i+1, length):
            a = candidates[i]
            b = candidates[j]
            prefix1 = a[:k-1]
            prefix2 = b[:k-1]

            if prefix1 == prefix2:
                temp = a + b[-1:]
                newCands.append(temp)

    return newCands


def pruneCandidates(dataset, candidates, min_sup):
    supCount = [0 for x in range(0, len(candidates))]

    for trans in dataset:
        i = 0
        for cands in candidates:
            tmp = frozenset(cands)
            if tmp.issubset(trans):
                supCount[i] += 1
            i += 1

    DBSize = float(len(dataset))
    newCands = []
    supList = []

    for id in range(0, len(supCount)):
        support = supCount[id] / DBSize
        if support >= min_sup:
            tmp = (candidates[id], supCount[id])
            supList.append(tmp)
            newCands.append(candidates[id])

    return newCands, supList


def main(argv):
    start = time.perf_counter()

    mDB = loadDataset(argv[1])

    minSup = float(argv[2])
    candsList = []
    allFrequent = []

    candsList = createCandidates(mDB)
    pruneList, allFrequent = pruneCandidates(mDB, candsList, minSup)

    while(len(pruneList) > 1):
        candsList = joinCandiates(pruneList)
        pruneList, tmpFrequent = pruneCandidates(mDB, candsList, minSup)
        allFrequent.extend(tmpFrequent)

    writeOutput("output", allFrequent)

    end = time.perf_counter()
    print("elasped time: %.8f second" % (end - start))

if __name__ == "__main__":
    main(sys.argv)
