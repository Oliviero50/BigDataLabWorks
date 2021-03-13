import sys
import os
import re

EXPECTED_NUM_OF_LINES = 100480507

## Big Data Group Kappa
## Authors: Weidele, Bauer, Pruell, Tomondy
def run():
    if len(sys.argv) < 3:
        print("Parameters missing")
        sys.exit(1)

    inputFileName = sys.argv[1]
    inputFile = open(inputFileName)

    outputFileName = sys.argv[2]
    outputFile = open(outputFileName, "w+")
 
    print("Is the file valid?")
    errors = isValid(inputFile)
    if(len(errors) == 0):
        print("Data is beautiful")
        outputFile.write("Data is beautiful")
    else:
        print("Data inconsistency found:")
        outputFile.write("Data inconsistency found: \n")
        for err in errors:
            print(err)
            outputFile.write(err + "\n")
    
    inputFile.close()
    outputFile.close()


##
# Checks for:
# Number of columns
# Data format
# Only valid characters
# Number of lines
# Returns list of errors
##
def isValid(file):
    numOfLines = 0
    errors = []
    ## Check for header
    firstLine = file.readline()

    for ch in firstLine:
        hasHeader = not ch.isdigit()
        break

    for ch in firstLine:     
        if not ch.isalnum(): 
            separator = ch
            break

    if not hasHeader: file.seek(0)

    while True:
        line = file.readline().rstrip()
        if not line:
            break
        numOfLines += 1 
        if(line.count(separator) != 3):
            errors.append("Invalid number of columns found on line " + str(numOfLines))
        if(line.count("-") != 2):
            errors.append("Date format error on line " + str(numOfLines))
        if(bool(re.match('^[0-9\-'+ separator +']+$', line)) == False): 
            errors.append("Invalid character error on line " + str(numOfLines))
            
    if(numOfLines != EXPECTED_NUM_OF_LINES):
        errors.append("Unexpected number of lines in the file!")

    return errors


if __name__ == "__main__":
    run()
