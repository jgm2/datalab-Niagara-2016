# Prepare a Niagara query output for XML parsing, then parse it and save in CSV format, sorted ascending on successive fields from the left.
# *** To be used to parse a truncated frames query ***
# (JGM) Jim Miller
# 09-30-13, 10-01-13, 11-2-13


import xml.etree.ElementTree as etree
from operator import itemgetter
import fileinput


inputFileName = 'D:/Query output/Frames,2nd-half_to_end-1800s - coarsened_by_100 - punctuated - 100B punc, TRUNCATED TO FRAMELENGTH OPERATOR.xml'
outputFile = open('D:/Query output/Frames,2nd-half_to_end-1800s - coarsened_by_100 - punctuated - 100B punc, TRUNCATED TO FRAMELENGTH OPERATOR - parsed.csv', 'w')

for line in fileinput.input(inputFileName, inplace = 1):
    if line.startswith('<niagara:results>'):
        line = line.replace("<niagara:results>", "<niagara:results xmlns:niagara=\"edu.pdx.cs.datalab.niagara\">")
    else:
        if line.startswith('Total time:'):
            line = line.replace("Total time:", "<!-- Total time:")
            line = line.replace("sec.", "sec. -->")
    print(line, end = "")
  
tree = etree.parse(inputFileName)
root = tree.getroot()
outputTagElements = ('pid', 'st_ts', 'end_ts', 'frameLength')
allCastChildLists = []

for outputTag in root:
    for outputChildList in zip(*[outputTag.findall(outputChildTag) for outputChildTag in outputTagElements]):
        if outputChildList[0].text == '*':      # Ignore any punctuation in the xml output from the Niagara query.
            continue
        outputCastChildList = []
        outputCastChildList.append(int(outputChildList[0].text))
        outputCastChildList.append(int(float(outputChildList[1].text)) - 1)
        outputCastChildList.append(int(float(outputChildList[2].text)) - 1)
        outputCastChildList.append(float(outputChildList[3].text))
        allCastChildLists.append(outputCastChildList)

allCastChildLists = sorted(allCastChildLists, key = itemgetter(0, 1, 2, 3))

for currentCastChildList in allCastChildLists:
    outputFile.write(",".join([repr(childTagText) for childTagText in currentCastChildList[0:4]]))
    outputFile.write("\n")

outputFile.close()
