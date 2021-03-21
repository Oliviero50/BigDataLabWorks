from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient("mongodb://localhost:27017")
db = client.movies
print("Quering data: ")


# f
# For the movie “The Spy Who Loved Me”:
# Get the average rating for each year and month sorted by year and month in
# ascending order
# movie_id = 17346

res = db.titles.find(
    {"title": "The Spy Who Loved Me"})

for r in res:
    print(r)
    print(r.get("_id"))

spy_id = 56

dontknow = db.reviews.aggregate(
    [
        {'$project': {
            'month': {'$month': '$time'},
            'year': {'$year': '$time'},
            'movie_id': '$movie_id',
            'time': '$time',
            'rating': '$rating'
        }
        },
        {'$match': {
            'movie_id': spy_id}  # lookup??
         },
        {'$group': {
            '_id': ['$year', '$month'],
            'avg': {'$avg': '$rating'}}
         },
        {'$sort': {'_id': 1}}
    ]
)

for d in dontknow:
    print(d)
