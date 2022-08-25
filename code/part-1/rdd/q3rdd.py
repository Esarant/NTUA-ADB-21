from pyspark.sql import SparkSession
import csv, time

spark = SparkSession.builder.appName("q3rdd").getOrCreate()
sc = spark.sparkContext

t0 = time.time()
# find average rating for each movie

# map(mid, (rating,1))
# reduce(mid, (total_ratings, total_rates))
# map(mid, movie_avg)
movie_avg = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
    map(lambda x : ( x.split(",")[1], (float(x.split(",")[2]),1) )). \
    reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
    map(lambda x: (x[0], (x[1][0]/x[1][1])))

# join movies with genres, find avg rating for each genre
# summing movie_avg rating/number of movies in genre

# map (mid, genre)
# join (mid, movie_avg)+(mid,genre) = (mid,(genre, movie_avg))
# map (genre, (movie_avg,1))
# reduce (genre, (genre_total, n_movies))
# answer: map (genre, (genre_avg, n_movies))
genre_avg = sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
    map(lambda x : (x.split(",")[0], x.split(",")[1])). \
    join(movie_avg). \
    map(lambda x: (x[1][0], (x[1][1], 1))). \
    reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
    map(lambda x: (x[0], (x[1][0]/x[1][1], x[1][1]))). \
    sortByKey().collect()

#add collect for print

print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("( genre, ( genre average rating, number of movie in genre))")
for i in genre_avg:
    print (i)

#genre_avg.saveAsTextFile("hdfs://master:9000/outputs/part-1/rdd/q3rdd-out.csv")
t1 = time.time()
t = t1 - t0

print("~~~~~~~~~~~~~~~~~~~~~~~~~~")
print(" Query ran in %.4f sec   |"%(t))
print("~~~~~~~~~~~~~~~~~~~~~~~~~~")

