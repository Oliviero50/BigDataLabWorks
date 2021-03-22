import sys
import os
from datetime import datetime, timedelta
from pymongo import MongoClient

BATCH_SIZE = 100_000


# Check args
if len(sys.argv) < 3:
    print("Eror: No filename to import provided")
    sys.exit(1)

# Get filename from sysargs
path = sys.argv[1]
movie_path = sys.argv[2]

if not os.path.exists(path) or not os.path.exists(movie_path):
    print("Error: Invalid filename")
    sys.exit(1)

rows = []
movie_rows = []

batch_counter = 1

client = MongoClient("mongodb://localhost:27017")
db = client.movies

print("!!! Clearing data in the titles collection !!!")
db.titles.delete_many({})

with open(movie_path, "r", encoding = "ISO-8859-1") as f:

    line = f.readline()

    while line:

        if not line:
            break

        line_ar = line.rstrip().split(",")

        if str(line_ar[1]) == "NULL":
            movie_rows.append(
                {"_id": int(line_ar[0]), "title": str(line_ar[2])})
        else:
            movie_rows.append({"_id": int(line_ar[0]), "year": int(
                line_ar[1]), "title": str(line_ar[2])})
        line = f.readline()

print("Inserting data innto db...")
result = db.titles.insert_many(movie_rows)
print("Inserted " + str(db.titles.count_documents({})) + " title documents")

print("!!! Clearing data in the reviews collection !!!")
db.reviews.delete_many({})

with open(path, "r") as f:
    while True:
        if batch_counter % BATCH_SIZE == 0:
            print("Processed " + str(batch_counter) + " lines")
            print("Writing to mongodb...")
            result = db.reviews.insert_many(rows)
            rows = []

        line = f.readline()
        if not line:
            break

        line = line.rstrip().split(",")
        date_of_rating = datetime.strptime(line[3], '%Y-%m-%d')

        rows.append({"movie_id": int(line[0]), "userd_id": int(
            line[1]), "rating": int(line[2]), "time": date_of_rating})
        batch_counter += 1

print("Inserted " + str(db.reviews.count_documents({})) + " documents")
print("Done")
