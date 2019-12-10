# Round the x and y coords in each input tuple down to cm, 10cm, etc., to coarsen the data.
# (JGM) Jim Miller
# 11-13-13


import fileinput
import sys

coarsenessFactor = 100
dataSeconds = 1800

inputFileName = "D:/Heatmap-input/2nd-half_to_end-" + str(dataSeconds) + "s.csv"
outputFile = open("D:/Heatmap-input/2nd-half_to_end-" + str(dataSeconds) + "s - coarsened_by_" + str(coarsenessFactor) + ".csv", 'w')
lineCount = 0

for line in fileinput.input(inputFileName):
    if lineCount == 0:
        outputFile.write(line)
        lineCount += 1
    else:
        lineAtts = line.split(',')
##        lineX = int(lineAtts[2])
##        if lineX >= 0:
##            lineAtts[2] = str(lineX // coarsenessFactor)
##        else:
##            lineAtts[2] = str(lineX // coarsenessFactor + 1)
        lineAtts[2] = str(int(lineAtts[2]) // coarsenessFactor * coarsenessFactor)
        lineAtts[3] = str(int(lineAtts[3]) // coarsenessFactor * coarsenessFactor)
        outputFile.write(",".join([attText for attText in lineAtts[0:13]]))
        lineCount += 1
        
outputFile.close()
