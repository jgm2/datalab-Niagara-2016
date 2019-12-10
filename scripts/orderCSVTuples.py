# Put in "tuple order" (sorted ascending on successive fields from the left) the csv tuples output from a reference tuple file.
# This is a script to fix broken reference output (because in the reference I was outputting the OOB tuple (with location values Integer.MIN_VALUE)
# AFTER the grid cell tuples, for a given ts and pid, and thus out of order, which confuses the final Python Niagara-reference comparison script.
# (JGM) Jim Miller
# 11-3-13


from operator import itemgetter
import fileinput

inputFileName = 'D:/Query output/HeatResultWithPartialsStream_16,25_300.csv'
outputFile = open('D:/Query output/HeatResultWithPartialsStream_16,25_300 - fixed.csv', 'w')
allLineAttValues = []

for line in fileinput.input(inputFileName):
    lineAtts = line.split(',')
    lineAttValues = []
    lineAttValues.append(int(float(lineAtts[0])) - 1)
    lineAttValues.append(int(lineAtts[1]))
    lineAttValues.append(float(lineAtts[2]))
    lineAttValues.append(float(lineAtts[3]))
    lineAttValues.append(float(lineAtts[4]))
    allLineAttValues.append(lineAttValues)

allLineAttValues = sorted(allLineAttValues, key = itemgetter(0, 1, 2, 3))

for currentLineAttValues in allLineAttValues:
    outputFile.write(",".join([repr(attText) for attText in currentLineAttValues[0:5]]))
    outputFile.write("\n")

outputFile.close()
