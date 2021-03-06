import sys
import os
import csv


def get_file_name(input_dir, i):
    return input_dir + "combined_data_" + str(i) + ".txt"


# say hello
print("\n*******************************")
print("Hello to kappa's merger!")
print("*******************************\n")

# read arguments, set input directory and output file
argList = list(sys.argv)
if len(argList) > 1:
    inputDir = argList[1]
    if not (os.path.isdir(inputDir)):
        exit()
    outputFile = argList[2]
    print("Dir and File argument ok!\n")
else:
    inputDir = ""
    outputFile = "_.csv"
    print("Standard Dir and File set!\n")

for i in [1, 2, 3, 4]:
    filepath = get_file_name(inputDir, i)
    if not os.path.isfile(filepath):
        print("At least one file missing! Program stops!")
        exit()
    else:
        print(filepath + " found")

print("\nStart creating CSV...\n")
with open(outputFile, 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, delimiter=';',
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)
    for filenumber in [1, 2, 3, 4]:
        filepath = get_file_name(inputDir, i)
        with open(filepath, "r") as file:
            for line in file:
                if line.__contains__("5:"):
                    break
                line = line.rstrip()
                if line.__contains__(":"):
                    movie_id = line.replace(":", "")
                    continue
                line = line.split(",")
                writer.writerow([int(movie_id)] + [line[0]] + [line[1]] + [line[2]])
        print(filepath + " done.")


# say bye
print("\n*******************************")
print("Finished merge. CSV saved to " + outputFile + ". See you!")
print("*******************************\n")