# Prepare a Niagara query output for XML parsing, then parse it and save in CSV format, sorted ascending on successive fields from the left.
# (JGM) Jim Miller
# 09-30-13, 10-01-13, 11-2-13


import xml.etree.ElementTree as etree
from operator import itemgetter
import fileinput


inputFileName = 'D:/Query output/FINAL testing/Frames,2nd-half_to_end-1800s - punctuated - 100B punc.xml'
outputFile = open('D:/Query output/FINAL testing/Frames,2nd-half_to_end-1800s - punctuated - 100B punc - parsed.csv', 'w')

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
outputTagElements = ('tsAtUpdate', 'pid', 'c_midx', 'c_midy', 'pidPercentTimeInCellPerWindow')
allCastChildLists = []

for outputTag in root:
    for outputChildList in zip(*[outputTag.findall(outputChildTag) for outputChildTag in outputTagElements]):
        if outputChildList[0].text == '*':      # Ignore any punctuation in the xml output from the Niagara query.
            continue
        outputCastChildList = []
        outputCastChildList.append(int(float(outputChildList[0].text)) - 1)
        outputCastChildList.append(int(outputChildList[1].text))
        outputCastChildList.append(float(outputChildList[2].text))
        outputCastChildList.append(float(outputChildList[3].text))
        outputCastChildList.append(float(outputChildList[4].text))
        allCastChildLists.append(outputCastChildList)

allCastChildLists = sorted(allCastChildLists, key = itemgetter(0, 1, 2, 3))

for currentCastChildList in allCastChildLists:
    outputFile.write(",".join([repr(childTagText) for childTagText in currentCastChildList[0:5]]))
    outputFile.write("\n")

outputFile.close()
