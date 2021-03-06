import sys
import os

## Big Data Group Kappa
## Authors: Weidele, Bauer, Pruell, Tomondy
def run():
    if len(sys.argv) < 3:
        print("Please provide a directory and output file name")
        sys.exit(1)

    path = sys.argv[1]
    if not os.path.isdir(path):
        print('Not a valid directory')
        sys.exit(1)

    filename = sys.argv[2]
    
    dir_cont = os.listdir(path)
    files = [x for x in dir_cont if x.startswith('combined_data_')]

    output = open(filename, "a+")

    for file in files:
        print("Working on file: " + file)
        file = open(os.path.join(path, file))
        movie_id = ''
        while True:
            line = file.readline().rstrip()
            if not line:
                break
            if line.endswith(':'):
                movie_id = line.split(':')[0]
                continue
            else:
                output.write(movie_id + ',' + line + '\n')
        file.close()
    output.close()


if __name__ == "__main__":
    run()

