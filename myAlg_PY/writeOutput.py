#!/bin/python


def writeOutput(filename, obj):
    fp = open(filename, "wt")

    for i in obj:
        fp.write(str(i[0]))
        fp.write(" : ")
        fp.write(str(i[1]))
        fp.write("\n")

    fp.close()
