from pyspark.sql import SparkSession
from io import StringIO
import csv, time

spark = SparkSession.builder.appName("qq5rdd").getOrCreate()

sc = spark.sparkContext

t0 = time.time()

def split_complex(x):
    ret = list(csv.reader(StringIO(x), delimiter=','))[0]
    return (ret[0], (ret[1], float(ret[7])))

def flatmap(a):
    res = []
    for item in a[1][0]:
        temp = (a[0], (item, a[1][1]))
        res.append(temp)
    return res

def fav(x, y):
    if (x[2] > y[2]):
        return x
    elif (x[2] < y[2]):
        return y
    elif (x[2] == y[2]):
        return x if(x[3] >= y[3]) else y

def worst(x, y):
    if (x[2] > y[2]):
        return y
    elif (x[2] < y[2]):
        return x
    elif (x[2] == y[2]):
        return x if(x[3] >= y[3]) else y

genres = \
        sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
        map(lambda x : (x.split(",")[0], x.split(",")[1]))

ratings = \
        sc.textFile("hdfs://master:9000/files/ratings.csv"). \
        map(lambda x : (x.split(",")[1], (x.split(",")[0], float(x.split(",")[2]), 1)))

#to rejoin data
ratings2 = ratings.map(lambda x: (x[1][0], (x[0], x[1][1])))

# map_movies (movie_id, (popularity, movie_name))
scores = sc.textFile("hdfs://master:9000/files/movies.csv") \
    .map(lambda x: split_complex(x))


topGenreUser = \
        genres.join(ratings). \
        map(lambda x: ((x[1][0], x[1][1][0]) , x[1][1][2])). \
        reduceByKey(lambda x, y: x+y). \
        map(lambda x: (x[0][0], ((x[0][1], ), x[1]))). \
        reduceByKey(lambda x, y: x if x[1]>y[1] else y). \
        flatMap(lambda x: flatmap(x)). \
        map(lambda x: (x[1][0], (x[0], x[1][1]))). \
        join(ratings2). \
        map(lambda x: (x[1][1][0], (x[1][0][0], x[0], x[1][1][1], x[1][0][1]))). \
        join(genres). \
        filter(lambda x: x[1][0][0] == x[1][1]). \
        join(scores). \
        map(lambda x: ((x[1][0][0][0], x[1][0][0][1], x[1][0][0][3]), (x[0], x[1][1][0], x[1][0][0][2], x[1][1][1])))
        


wMovieUser = topGenreUser.reduceByKey(lambda x, y: worst(x, y))

# rdd3 (category, user_id, $ratings, most_fav_movie_id, most_fav_movie_name, rating, least_fav_movie_id, least_fav_movie_name, rating)
res = topGenreUser.reduceByKey(lambda x, y: fav(x, y)). \
    join(wMovieUser). \
    sortByKey(). \
    map(lambda x: (x[0][0], x[0][1], x[0][2], x[1][0][1], x[1][0][2], x[1][1][1], x[1][1][2])). \
    collect()
    
print("Genre, User_id, Number of Ratings, Favorite Movie, Favorite Rating, Worst Movie, Worst Rating")
for i in res:
     print(i)

#res.saveAsTextFile("hdfs://master:9000/outputs/part-1/rdd/q5rdd-out.csv")

t1 = time.time()
t = t1 - t0

print("~~~~~~~~~~~~~~~~~~~~~~~~~~")
print(" Query ran in %.4f sec  |"%(t))
print("~~~~~~~~~~~~~~~~~~~~~~~~~~")


