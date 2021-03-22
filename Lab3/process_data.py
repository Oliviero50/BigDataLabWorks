from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient("mongodb://localhost:27017")
db = client.movies
print("Quering data: ")

# a
# Compute the average rating, lowest rating, highest rating and
# number of ratings per movie

moviegroup = db.reviews.aggregate([
    { '$group':
        { '_id': "$movie_id",
            "count": 
                { '$sum' : 1 },
            "average":
                { '$avg': '$rating'},
            "min":
                { '$min': '$rating'},
            "max":
                { '$max': '$rating'}
        }
    },
    {"$sort":  { "_id": 1}},
        {'$lookup': {'from': 'titles', 'localField': '_id', 'foreignField': '_id', 'as': 'title'}},
        {'$project': {'overall': 1, 'title.title': 1, 'çount': 1, 'average': 1, 'min': 1, 'max': 1, '_id': 0}}
] )

for group in moviegroup:
    print(group)


# b
# Get the movie with the lowest overall rating.
# If the movies are tied for rating, take the movie with the lowest movie_id

lowestrating = db.reviews.aggregate(
    [
        {'$group':
         {'_id': '$movie_id',
          'overall':
          {'$avg': {'$sum': '$rating'}}
          }},
        {'$sort': {'overall': 1, '_id': 1}},
        {'$limit': 1},
        {'$lookup': {'from': 'titles', 'localField': '_id', 'foreignField': '_id', 'as': 'title'}},
        {'$project': {'overall': 1, 'title.title': 1, '_id': 0}}
    ])

print("Move with the lowest rating: ")
for group in lowestrating:
    print(group)

# c
# Get the movie with the highest overall rating.
# If the movies are tied for rating, take the movie with the lowest movie_id

highestrating = db.reviews.aggregate(
    [
        {'$group':
         {'_id': '$movie_id',
          'overall':
          {'$avg': {'$sum': '$rating'}}
          }},
        {'$sort': {'overall': -1, '_id': 1}},
        {'$limit': 1},
        {'$lookup': {'from': 'titles', 'localField': '_id', 'foreignField': '_id', 'as': 'title'}},
        {'$project': {'overall': 1, 'title.title': 1, '_id': 0}}
    ])

print("Move with the highest rating: ")
for group in highestrating:
    print(group)

# d
# Get the number of ratings in February 2002
start_date = datetime.strptime('2002-02-01', '%Y-%m-%d')
end_date = datetime.strptime('2002-02-28', '%Y-%m-%d')

query = {"time": {"$gte": start_date, "$lte": end_date}}
sum_ratings_feb = db.reviews.count_documents(query)

print("Number of rating in February 2002:", sum_ratings_feb)

# e
# Get the user that creates the lowest average rating and has rated at least 5 times.
# If two users are tied, use the one with the lower user_id

lowest_user_rating = db.reviews.aggregate(
    [
        {'$group':
         {'_id': '$userd_id',
          'overall':
          {'$avg': {'$sum': '$rating'}}
          }},
        {'$sort': {'overall': 1, '_id': 1}},
        {'$limit': 1}

    ])

print("User that gives the lowest rating: ")
for group in lowest_user_rating:
    print(group)

# f
# For the movie “The Spy Who Loved Me”:
# Get the average rating for each year and month sorted by year and month in
# ascending order
# movie_id = 17346

result = db.titles.find({"title": "The Spy Who Loved Me"})

for r in result:
    spy_id = r.get("_id")

average_ratings_over_time = db.reviews.aggregate(
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
            'movie_id': spy_id}
         },
        {'$group': {
            '_id': ['$year', '$month'],
            'avg': {'$avg': '$rating'}}
         },
        {'$sort': {'_id': 1}}
    ]
)

print("Average ratings over time:")
for r in average_ratings_over_time:
    print(r)
