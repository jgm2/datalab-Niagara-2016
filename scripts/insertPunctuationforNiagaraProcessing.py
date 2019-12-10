# Insert appropriate punctuation into the input stream of a Niagara heatmap query, at 1/10 s (100 billion picoseconds) intervals, in order to
# make the query's aggregation operators fire and avoid GC memory errors when processing large (5 million+) input streams.
#
# We make the assumption that the first bucket/longBucket operator encountered in the frames or windows heatmap queries has a slide that is a multiple of 1/10 s.
#
# (JGM) Jim Miller
# 10-16-13, 11-1-13, 11-3-13


import fileinput

inputStreamFileName = 'D:/Heatmap-input/2nd-half_to_end-1800s - coarsened_by_10.csv'
outputStreamFile = open('D:/Heatmap-input/2nd-half_to_end-1800s - coarsened_by_10 - punctuated - 100B punc.csv', 'w')
startTS = 13086639146403495
interval = 100000000000         # 1/10 s
newBoundary = 13086639146403495 + interval
lineCounter = 0

for line in fileinput.input(inputStreamFileName):
    lineCounter += 1
    if lineCounter == 1:
        outputStreamFile.write(line)
        continue
    attribs = line.split(',')
    # Amended 11-3-13 to test if re-ordering the tuples at these boundary situations is important.
    # 11-4-13:  Discovered that this will break Niagara windows heatmap query - null pointer, blah.  Is it really this script's fault, or Niagara?
    #if int(attribs[1]) == newBoundary:
    #    outputStreamFile.write(line)
    #    punctLine = '*,' + str(newBoundary) + ',*,*,*,*,*,*,*,*,*,*,*'
    #    outputStreamFile.write(punctLine + '\n')
    #    newBoundary += interval
    #if int(attribs[1]) > newBoundary:
    if int(attribs[1]) > newBoundary:
        punctLine = '*,' + str(newBoundary) + ',*,*,*,*,*,*,*,*,*,*,*'
        outputStreamFile.write(punctLine + '\n')
        newBoundary += interval
        outputStreamFile.write(line)
    else:
        outputStreamFile.write(line)

outputStreamFile.close()
