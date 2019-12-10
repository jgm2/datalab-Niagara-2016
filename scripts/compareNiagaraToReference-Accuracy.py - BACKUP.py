# Quantify Niagara frames vs. windows query performance.
# Run final comparisons between Niagara frame query output and reference output, and between Niagara windows query output and reference output,
# evaluating average and max differences for each.
# (JGM) Jim Miller
# 10-15-13, 10-16-13, 11-2-13

import fileinput
import math
import sys

niagaraFramesFileName = 'D:/Query output/Frames,2nd-half_to_end-300s - punctuated - 100B punc - parsed.csv'
niagaraWindowsFileName = 'D:/Query output/Windows,2nd-half_to_end-300s - punctuated - 100B punc, .1sec window - parsed.csv'
referenceFileName = 'D:/Query output/HeatResultWithPartialsStream_16,25_60.csv'
fraTupleCount = 0
fraDiffMax = 0
fraDiffAvg = 0
winTupleCount = 0
winDiffMax = 0
winDiffAvg = 0

fraFile = open(niagaraFramesFileName, 'r')
fraLine = fraFile.readline()
fraAtts = fraLine.split(',')
winFile = open(niagaraWindowsFileName, 'r')
winLine = winFile.readline()
winAtts = winLine.split(',')
for refLine in fileinput.input(referenceFileName):
    refAtts = refLine.split(',')
    refTS = int(float(refAtts[0]))
    refPID = int(refAtts[1])
    refX = math.floor(float(refAtts[2]))
    refY = math.floor(float(refAtts[3]))
    refPCT = float(refAtts[4])
    #sys.stdout.write(str(refTS) + "," + str(refPID) + "," + str(refX) + "," + str(refY) + "," + str(refPCT) + "\n")
    if (refTS == int(float(fraAtts[0])) and refPID == int(fraAtts[1]) and refX == math.floor(float(fraAtts[2])) and refY == math.floor(float(fraAtts[3]))) \
     or (refPCT != float(0) and (refTS > int(float(fraAtts[0])) or refPID > int(fraAtts[1]) or refX == math.floor(float(fraAtts[2])) or refY == math.floor(float(fraAtts[3])))):
        if refPCT != float(0) or float(fraAtts[4]) != float(0):                             # Is 0. the correct way to indicate a float?
            fraDiff = math.fabs(refPCT - float(fraAtts[4]))
            if fraDiff > fraDiffMax:
                fraDiffMax = fraDiff;
            fraDiffAvg = (fraDiffAvg * fraTupleCount + fraDiff) / (fraTupleCount + 1)
            fraTupleCount += 1
        fraAtts = fraFile.readline().split(',')
    if (refTS == int(float(winAtts[0])) and refPID == int(winAtts[1]) and refX == math.floor(float(winAtts[2])) and refY == math.floor(float(winAtts[3]))) \
     or (refPCT != float(0) and (refTS > int(float(winAtts[0])) or refPID > int(winAtts[1]) or refX == math.floor(float(winAtts[2])) or refY == math.floor(float(winAtts[3])))):
        if refPCT != float(0) or float(winAtts[4]) != float(0):                             # Is 0. the correct way to indicate a float?
            winDiff = math.fabs(refPCT - float(winAtts[4]))
            if winDiff > winDiffMax:
                winDiffMax = winDiff;
            winDiffAvg = (winDiffAvg * winTupleCount + winDiff) / (winTupleCount + 1)
            winTupleCount += 1
        winAtts = winFile.readline().split(',')

sys.stdout.write("Frames query difference from reference max:  " + str(fraDiffMax) + "\n")
sys.stdout.write("Frames query difference from reference avg:  " + str(fraDiffAvg) + "\n")
sys.stdout.write("Windows query difference from reference max:  " + str(winDiffMax) + "\n")
sys.stdout.write("Windows query difference from reference avg:  " + str(winDiffAvg) + "\n")

fraFile.close()
winFile.close()
