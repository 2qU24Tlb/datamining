import sys
from common.vitemset import *


class Eclat():
    def __init__(self, DB, minSup):
        self.DB = DB.mdb
        self.minSup = len(DB.mdb) * minSup
        self.allFrequent = []

    def CreateCandidates(self):
        freList = []
        items = self.DB.items()

        for i in items:
            if (len(i[1].tids) > self.minSup):
                for j in range(len(freList)):
                    if (freList[j].name > i[1].name):
                        freList.insert(j, i[1])
                        break
                else:
                    freList.append(i[1])
                self.allFrequent.append(i[1])

        return freList

    def IntersectItems(self, freList):
        for i in range(len(freList)):
            newFreList = []
            for j in range(i+1, len(freList)):
                result = Vitemset()
                result.name = freList[i].name + freList[j].name[-1]
                result.tids = freList[i].tids.intersection(freList[j].tids)
                if (len(result.tids) > self.minSup):
                    newFreList.append(result)
                    self.allFrequent.append(result)

            if (newFreList):
                Eclat.IntersectItems(self, newFreList)

    def PrintResult(self):
        for i in self.allFrequent:
            print(i.name + ":" + str(i.tids))


def main(argv):
    mDB = CreateVDB(argv[1])
    minSup = float(argv[2])
    mEclat = Eclat(mDB, minSup)
    c1 = mEclat.CreateCandidates()
    mEclat.IntersectItems(c1)
    mEclat.PrintResult()


if __name__ == "__main__":
    main(sys.argv)
