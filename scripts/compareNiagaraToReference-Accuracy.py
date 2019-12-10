# Quantify Niagara frames vs. windows query performance.
# Run final comparisons between Niagara frame query output and reference output, and between Niagara windows query output and reference output,
# evaluating average and max differences for each.
#
# (JGM) Jim Miller
# 10-15-13, 10-16-13, 11-2-13, 11-3-13, 11-16-13


import fileinput
import math
import sys

niagaraFramesFileName = 'D:/Query output/FINAL testing/Frames,2nd-half_to_end-60s - punctuated - 100B punc - parsed.csv'
niagaraWindowsFileName = 'D:/Query output/FINAL testing/Windows,2nd-half_to_end-60s - punctuated - 100B punc, .5sec window - parsed.csv'
referenceFileName = 'D:/Query output/FINAL testing/HeatResultWithPartialsStream_60s_16,25_60.csv'
fraTupleCount = 0
fraDiffMax = 0
fraDiffAvg = 0
winTupleCount = 0
winDiffMax = 0
winDiffAvg = 0
fraCombined = False

fraFile = open(niagaraFramesFileName, 'r')
fraAtts = fraFile.readline().split(',')
fraTS = int(float(fraAtts[0])) - 1
fraPID = int(fraAtts[1])
# All truncating should go in the same direction.  Otherwise, values between -1 and 1 get lumped together in a double-sized range! (11-16-13)
##if float(fraAtts[2]) >= float(0):
##    fraX = math.floor(float(fraAtts[2]))
##else:
##    fraX = math.ceil(float(fraAtts[2]))
##if float(fraAtts[3]) >= float(0):
##    fraY = math.floor(float(fraAtts[3]))
##else:
##    fraY = math.ceil(float(fraAtts[3]))
fraX = math.floor(float(fraAtts[2]))
fraY = math.floor(float(fraAtts[3]))
fraPCT = float(fraAtts[4])

# This is a fix to check for two (but not more!!!) consecutive lines with the same "signature" (ts, pid, cellMidX, cellMidY), resulting from a bug
# somewhere in a frames (**NOT windows**) Niagara heatmap operator, probably one of mine or in windowLongSum....the bug may have to do with how I punctuated,
# and/or with how I handled events right at the window boundaries (punctuation, how I ordered it in relation to data tuples, and perhaps inconsistencies,
# across my operators and the reference program, in the conception of whether the first or second window "owns" the boundary....just guessing here).
fraNewAtts = fraFile.readline().split(',')
fraNewTS = int(float(fraNewAtts[0])) - 1
fraNewPID = int(fraNewAtts[1])
##if float(fraNewAtts[2]) >= float(0):
##    fraNewX = math.floor(float(fraNewAtts[2]))
##else:
##    fraNewX = math.ceil(float(fraNewAtts[2]))
##if float(fraNewAtts[3]) >= float(0):
##    fraNewY = math.floor(float(fraNewAtts[3]))
##else:
##    fraNewY = math.ceil(float(fraNewAtts[3]))
fraNewX = math.floor(float(fraNewAtts[2]))
fraNewY = math.floor(float(fraNewAtts[3]))
fraNewPCT = float(fraNewAtts[4])
if fraTS == fraNewTS and fraPID == fraNewPID and fraX == fraNewX and fraY == fraNewY:
    fraPCT = fraPCT + fraNewPCT
    fraCombined = True

winFile = open(niagaraWindowsFileName, 'r')
winAtts = winFile.readline().split(',')
winTS = int(float(winAtts[0])) - 1
winPID = int(winAtts[1])
##if float(winAtts[2]) >= float(0):
##    winX = math.floor(float(winAtts[2]))
##else:
##    winX = math.ceil(float(winAtts[2]))
##if float(winAtts[3]) >= float(0):
##    winY = math.floor(float(winAtts[3]))
##else:
##    winY = math.ceil(float(winAtts[3]))
winX = math.floor(float(winAtts[2]))
winY = math.floor(float(winAtts[3]))
winPCT = float(winAtts[4])

for refLine in fileinput.input(referenceFileName):
    refAtts = refLine.split(',')
    refTS = int(float(refAtts[0])) - 1
    refPID = int(refAtts[1])
    # More contingency handling for differences between reference and Niagara query output.  The reference may have Integer.MIN_VALUE in scientific notation.
    if refAtts[2] == "-2.14748365E9":
        refAtts[2] = "-2147483648.0"
##    if float(refAtts[2]) >= float(0):
##        refX = math.floor(float(refAtts[2]))
##    else:
##        refX = math.ceil(float(refAtts[2]))
    refX = math.floor(float(refAtts[2]))
    if refAtts[3] == "-2.14748365E9":
        refAtts[3] = "-2147483648.0"
##    if float(refAtts[3]) >= float(0):
##        refY = math.floor(float(refAtts[3]))
##    else:
##        refY = math.ceil(float(refAtts[3]))
    refY = math.floor(float(refAtts[3]))
    refPCT = float(refAtts[4])
    
    fraLineMatch = (refTS == fraTS and refPID == fraPID and refX == fraX and refY == fraY)
    if fraLineMatch or (refPCT != float(0) and not fraLineMatch and refPID <= 16):
        if refPCT != float(0) or fraPCT != float(0):                             # Is 0. the correct way to indicate a float?
            fraDiff = math.fabs(refPCT - fraPCT)
            if fraDiff > fraDiffMax:
                fraDiffMax = fraDiff;
            fraDiffAvg = (fraDiffAvg * fraTupleCount + fraDiff) / (fraTupleCount + 1)
            fraTupleCount += 1
        if fraLineMatch:

            # More fix for the Niagara bug.
            if not fraCombined:
                fraTS = fraNewTS
                fraPID = fraNewPID
                fraX = fraNewX
                fraY = fraNewY
                fraPCT = fraNewPCT
            else:
                fraCombined = False
                fraAtts = fraFile.readline().split(',')
                fraTS = int(float(fraAtts[0])) - 1
                fraPID = int(fraAtts[1])
##                if float(fraAtts[2]) >= float(0):
##                    fraX = math.floor(float(fraAtts[2]))
##                else:
##                    fraX = math.ceil(float(fraAtts[2]))
##                if float(fraAtts[3]) >= float(0):
##                    fraY = math.floor(float(fraAtts[3]))
##                else:
##                    fraY = math.ceil(float(fraAtts[3]))
                fraX = math.floor(float(fraAtts[2]))
                fraY = math.floor(float(fraAtts[3]))
                fraPCT = float(fraAtts[4])
            fraNewAtts = fraFile.readline().split(',')
            fraNewTS = int(float(fraNewAtts[0])) - 1
            fraNewPID = int(fraNewAtts[1])
##            if float(fraNewAtts[2]) >= float(0):
##                fraNewX = math.floor(float(fraNewAtts[2]))
##            else:
##                fraNewX = math.ceil(float(fraNewAtts[2]))
##            if float(fraNewAtts[3]) >= float(0):
##                fraNewY = math.floor(float(fraNewAtts[3]))
##            else:
##                fraNewY = math.ceil(float(fraNewAtts[3]))
            fraNewX = math.floor(float(fraNewAtts[2]))
            fraNewY = math.floor(float(fraNewAtts[3]))
            fraNewPCT = float(fraNewAtts[4])
            if fraTS == fraNewTS and fraPID == fraNewPID and fraX == fraNewX and fraY == fraNewY:
                fraPCT = fraPCT + fraNewPCT
                fraCombined = True
            #sys.stdout.write("fra:  " + str(fraTS) + "," + str(fraPID) + "," + str(fraX) + "," + str(fraY) + "," + str(fraPCT) + "\n")
            
    winLineMatch = (refTS == winTS and refPID == winPID and refX == winX and refY == winY)
    if winLineMatch or (refPCT != float(0) and not winLineMatch and refPID <= 16):
        if refPCT != float(0) or winPCT != float(0):                             # Is 0. the correct way to indicate a float?
            winDiff = math.fabs(refPCT - winPCT)
            if winDiff > winDiffMax:
                winDiffMax = winDiff;
            winDiffAvg = (winDiffAvg * winTupleCount + winDiff) / (winTupleCount + 1)
            winTupleCount += 1
        if winLineMatch:
            winAtts = winFile.readline().split(',')
            winTS = int(float(winAtts[0])) - 1
            winPID = int(winAtts[1])
##            if float(winAtts[2]) >= float(0):
##                winX = math.floor(float(winAtts[2]))
##            else:
##                winX = math.ceil(float(winAtts[2]))
##            if float(winAtts[3]) >= float(0):
##                winY = math.floor(float(winAtts[3]))
##            else:
##                winY = math.ceil(float(winAtts[3]))
            winX = math.floor(float(winAtts[2]))
            winY = math.floor(float(winAtts[3]))
            winPCT = float(winAtts[4])
            #sys.stdout.write("win:  " + str(winTS) + "," + str(winPID) + "," + str(winX) + "," + str(winY) + "," + str(winPCT) + "\n")

sys.stdout.write("Frames, reference diff MAX:  " + str(fraDiffMax) + "\n")
sys.stdout.write("Frames, reference diff AVG:  " + str(fraDiffAvg) + "\n")
sys.stdout.write("Windows, reference diff MAX:  " + str(winDiffMax) + "\n")
sys.stdout.write("Windows, reference diff AVG:  " + str(winDiffAvg) + "\n")

fraFile.close()
winFile.close()
