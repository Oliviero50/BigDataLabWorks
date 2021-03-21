import sys
import os
from datetime import datetime, timedelta
from pymongo import MongoClient

BATCH_SIZE = 100_000 #1_000_000



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
print("!!! Clearing data in the movies collection !!!")
db.reviews.delete_many({})

with open(path, "r") as f:
    while True:
        if batch_counter % BATCH_SIZE == 0:
            print("Processed " + str(batch_counter) + " lines")
            print("Writing to mongodb...")
            result = db.reviews.insert_many(rows)
            rows = []

        # TODO: Remove
        if batch_counter > 500_000: ### 10_000_000
            break

        line = f.readline()
        if not line:
            break
        
        line = line.rstrip().split(",")
        ### Save date as a datetime object
        date_of_rating = datetime.strptime(line[3], '%Y-%m-%d')

        rows.append({"movie_id": int(line[0]), "userd_id": int(line[1]), "rating": int(line[2]), "time": date_of_rating})
        batch_counter += 1

print("Inserted " + str(db.reviews.count_documents({})) + " documents")
print("Done")