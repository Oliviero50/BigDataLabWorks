from pymongo import MongoClient
from datetime import datetime, timedelta

## TEST FILE ## 

client = MongoClient("mongodb://localhost:27017")
db = client.movies
print("Reading")

# Get the number of ratings

num_ratings = db.reviews.count_documents({})

print("Number of ratings in Febraury 2002: ")
print(num_ratings)


## Find one specific by id
find_one = db.reviews.find_one({
    'movie_id': { '$eq': 9224}
})
print(find_one)

## Find every rating specific by date
find_dates = db.reviews.find({
    'time': { '$eq': datetime.strptime('2005-07-08', '%Y-%m-%d')}
})
#for movie in find_dates:
#    print(movie)


## Count every rating in specific date range
start_date = datetime.strptime('2002-02-01', '%Y-%m-%d')
end_date = datetime.strptime('2002-02-28', '%Y-%m-%d')

query = {"time": {"$gte": start_date, "$lte": end_date}}
find_rating_in_feb = db.reviews.count_documents(query)
print(find_rating_in_feb)

