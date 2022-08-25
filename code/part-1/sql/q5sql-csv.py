from pyspark.sql import SparkSession
import sys, time
spark = SparkSession.builder.appName("q5sql-pqt").getOrCreate()

#parquet only

t0 = time.time()

movies = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/movies.csv")
movie_genres = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/movie_genres.csv")
ratings = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/ratings.csv")

movies.registerTempTable("movies")
movie_genres.registerTempTable("movie_genres")
ratings.registerTempTable("ratings")

sqlString = \
        "SELECT b.Genre, FIRST(b.UserId) as UserId, FIRST(b.n_ratings) as numberOfRatings, FIRST(b.Title) as favoriteMovie, FIRST(b.Rating) as FavoriteRating, \
        FIRST(b.worst) as worstMovie, FIRST(b.wRate) as worstRating\
        FROM\
        (SELECT a.Genre, a.UserId, a.wRate, a.n_ratings, a.Title, a.Rating, a.fRate, max(n_ratings) over (PARTITION BY Genre) as MaxSum, a.fav, a.worst\
        FROM\
            (SELECT g._c1 as Genre, r._c0 as UserId, m._c1 as Title, r._c2 as Rating,\
                COUNT(r._c2) over (PARTITION BY g._c1, r._c0) as n_ratings, \
                FIRST(r._c2) over (PARTITION BY g._c1, r._c0 ORDER BY r._c2 DESC, m._c7 DESC) as fRate,\
                FIRST(m._c1) over (PARTITION BY g._c1, r._c0 ORDER BY r._c2 DESC, m._c7 DESC) as fav, \
                FIRST(r._c2) over (PARTITION BY g._c1, r._c0 ORDER BY r._c2 ASC, m._c7 DESC) as wRate,\
                FIRST(m._c1) over (PARTITION BY g._c1, r._c0 ORDER BY r._c2 ASC, m._c7 DESC) as worst\
            FROM movie_genres as g INNER JOIN ratings as r ON g._c0 = r._c1 INNER JOIN movies as m ON m._c0 = r._c1\
            )a\
        )b\
        WHERE b.n_ratings = b.MaxSum and b.fRate = b.Rating\
        GROUP BY b.Genre\
        ORDER BY b.Genre ASC"


res = spark.sql(sqlString)
#res.show()


res.write.csv("hdfs://master:9000/outputs/part-1/sql/csv/q5sql-csv.csv")

t1 = time.time()
t = t1 - t0

print("~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("query ran in %.4f sec  |"%(t))
print("~~~~~~~~~~~~~~~~~~~~~~~~~~")

