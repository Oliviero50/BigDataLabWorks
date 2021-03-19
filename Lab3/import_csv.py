import sys
import os
from pymongo import MongoClient

BATCH_SIZE = 1_000_000



# Check args
if len(sys.argv) < 2:
    print("Eror: No filename to import provided")
    sys.exit(1)
# Get filename from sysargs
path = sys.argv[1]
if not os.path.exists(path):
    print("Error: Invalid filename")
    sys.exit(1)

rows = []
batch_counter = 1

client = MongoClient("mongodb://localhost:27017")
db = client.movies

with open(path, "r") as f:
    while True:
        if batch_counter % BATCH_SIZE == 0:
            print("Processed " + str(batch_counter) + " lines")
            print("Writing to mongodb...")
            result = db.reviews.insert_many(rows)
            rows = []

        if batch_counter > 10_000_000:
            break

        line = f.readline()
        if not line:
            break
        
        line = line.rstrip().split(",")

        rows.append({"movie_id": int(line[0]), "userd_id": int(line[1]), "rating": int(line[2]), "time": line[3]})
        batch_counter += 1

print("Inserted " + str(db.reviews.count_documents({})) + " documents")
print("Done")