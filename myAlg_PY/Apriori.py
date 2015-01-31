#!/bin/python

#  Modified from:
#  https://github.com/cse40647/cse40647/blob/sp.14/10%20-%20Apriori.ipynb

import pprint
import sys
from loadDataset import loadDataset

def createCandidates(dataset):
    c1 = []

    for transaction in dataset:
        for item in transaction:
            if [item] not in c1:
                c1.append([item])
    c1.sort()

    return [frozenset(x) for x in c1]

def pruneCandidates(dataset, candidates, min_sup):
    supCount = {} 
    for trans in dataset:
        for cands in candidates:
            if cands.issubset(trans):
                supCount.setdefault(cands, 0)
                supCount[cands] += 1

    DBSize = float(len(dataset))
    newCands = []
    newCandsDict = {}

    for sets in supCount:
        support = supCount[sets] / DBSize
        if support >= min_sup:
            newCands.insert(0, sets)
            newCandsDict[sets] = support

    #newCands.sort()
    for item in newCands:
        print(item)
    #pprint.pprint(newCands)

    return newCands, newCandsDict

def joinCandiates(candidates):
    newCands = []
    k = len(candidates[0]) 
    length = len(candidates)

    for i in range(length):
        for j in range(i+1, length):
            a = list(candidates[i])
            b = list(candidates[j])
            #[FIXME] for long list sorting will be a problem
            a.sort()
            b.sort()

            prefix1 = a[:k-1]
            prefix2 = b[:k-1]

            if prefix1 == prefix2:
                newCands.append(candidates[i] | candidates[j])

    return newCands

def main(argv):
    newDataset = loadDataset(argv[1])
    #print("\n-->DATA SET:")
    #pprint.pprint(newDataset)

    minSup = float(argv[2])

    candidates = createCandidates(newDataset)
    newCands, newCandsDict = pruneCandidates(newDataset, candidates, minSup)

    while(len(newCands) > 1):
        newCands = joinCandiates(newCands)
        newCands, tempCandsDict = pruneCandidates(newDataset, newCands, minSup)
        newCandsDict.update(tempCandsDict)
        print(newCands)

    #print("-->Result:")
    #pprint.pprint(newCandsDict)

if __name__ == "__main__":
    main(sys.argv)
