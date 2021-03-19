from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client.movies

# Get one
#result = db.reviews.find_one({'rating': 5})
#print(result)

# Count with filter
#result = db.reviews.count_documents({'rating': 5})
#print(result)

# Aggregate pipeline
#stargroup = db.reviews.aggregate(
# The Aggregation Pipeline is defined as an array of different operations
#[
# The first stage in this pipe is to group data
#{ '$group':
#    { '_id': "$rating",
#     "count" : 
#                 { '$sum' :1 }
#    }
#},
# The second stage in this pipe is to sort the data
#{"$sort":  { "_id":1}
#}
# Close the array with the ] tag             
#] )
# Print the result
#for group in stargroup:
#    print(group)

#a
# Compute the average rating, lowest rating, highest rating and
# number of ratings per movie

""" moviegroup = db.reviews.aggregate([
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
    {"$sort":  { "_id": 1}}          
] )

for group in moviegroup:
    print(group) """


#b
# Get the movie with the lowest overall rating.
# If the movies are tied for rating, take the movie with the lowest movie_id

lowestrating = db.reviews.aggregate(
[
{   '$group':
        {   '_id': '$movie_id',
            'overall':
                {'$avg': {'$sum': '$rating'}}
        }},
        {'$sort': {'overall': 1}},
        {'$limit': 1}
])

for group in lowestrating:
    print(group)


#c
# Get the movie with the highest overall rating.
# If the movies are tied for rating, take the movie with the lowest movie_id

highestrating = db.reviews.aggregate(
[
{   '$group':
        {   '_id': '$movie_id',
            'overall':
                {'$avg': {'$sum': '$rating'}}
        }},
        {'$sort': {'overall': -1}},
        {'$limit': 1}
])

for group in highestrating:
    print(group)

#d
# Get the number of ratings in February 2002

#e
# Get the user that creates the lowest average rating and has rated at least 5 times.
# If two users are tied, use the one with the lower user_id

#f
# For the movie “The Spy Who Loved Me”:
# Get the averagerating for each year and month sorted by year and month in
# ascending order