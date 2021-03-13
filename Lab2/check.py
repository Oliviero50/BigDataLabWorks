import sys
import os
import re

## Big Data Group Kappa
## Authors: Weidele, Bauer, Pruell, Tomondy
def run():
    if len(sys.argv) < 1:
        print("Please provide the input file name")
        sys.exit(1)

    path = sys.argv[0]
    if not os.path.isfile(path):
        print('Not a file')
        sys.exit(1)


    filename = sys.argv[0]
    file = open(filename)
    print("Is the file valid?")
    print(isValid(file))

##
# Checks for:
# Number of commas
# Number of hayfns
# Only numbers, commas, hayfns
# TODO: Number of lines
##
def isValid(file):
    for line in file:
        if(line.count(",") != 3): 
            return False
        if(line.count("-") != 2): 
            return False
        if(bool(re.match('^[1234567890\-,]+$', line)) == False): 
            return False   
    return True


if __name__ == "__main__":
    run()
