class Vitemset:
    "Vertical Itemset class"
    def __init__(self):
        self.tids = set()
        self.name = ""


class VDB:
    "Vertical Database"
    mdb = {}

    def addItem(self, name, tid):
        if (name not in VDB.mdb):
            newItem = Vitemset()
            VDB.mdb[name] = newItem
            VDB.mdb[name].name = name

        VDB.mdb[name].tids.add(tid)

    def printDB(self):
        itemList = VDB.mdb.items()
        for i in itemList:
            print(i[1].name, list(i[1].tids))


def CreateVDB(filename):
    newDB = VDB()

    fp = open(filename, 'rt')

    lineNo = 0
    for line in fp:
        lineNo += 1
        itemlist = line.split()
        for i in itemlist:
            newDB.addItem(i, lineNo)

    fp.close()

    return newDB
