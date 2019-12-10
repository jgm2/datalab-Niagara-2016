# Get the average frameLength after the frameLength operator in the frames version of the DEBS heatmap query.
# For helping to determine what length of windows in the windows version of the query might make a difference in accuracy measurement.
#
# (JGM) Jim Miller
# 11-8-13


import sys

niagaraFramesFileName = "D:/Query output/Frames,2nd-half_to_end-1800s - punctuated - 100B punc, TRUNCATED TO FRAMELENGTH OPERATOR - parsed.csv"
fraTupleCount = 0
fraLengthMin = 9999999
fraLengthMax = -9999999
fraLengthAvg = 0

fraFile = open(niagaraFramesFileName, 'r')
fraLine = fraFile.readline()

while 1:
    if not fraLine:
        break
    fraAtts = fraLine.split(',')
    fraLength = float(fraAtts[3])
    if fraLength > fraLengthMax:
        fraLengthMax = fraLength
    else:
        if fraLength < fraLengthMin:
            fraLengthMin = fraLength
    fraLengthAvg = (fraLengthAvg * fraTupleCount + fraLength) / (fraTupleCount + 1)
    fraTupleCount += 1
    fraLine = fraFile.readline()
    #sys.stdout.write("fra:  " + str(fraAtts[0]) + str(fraAtts[1]) + str(fraAtts[2]) + str(fraLength) + "\n")

sys.stdout.write("Frames, frameLength MIN:  " + str(fraLengthMin / 1000000000000) + "\n")
sys.stdout.write("Frames, frameLength MAX:  " + str(fraLengthMax / 1000000000000) + "\n")
sys.stdout.write("Frames, frameLength AVG:  " + str(fraLengthAvg / 1000000000000) + "\n")

fraFile.close()
