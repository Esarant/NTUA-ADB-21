from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("q3sql").getOrCreate()

'''
 select file type between "csv" and "parquet"
 $ spark-submit q1sql.py csv
 would read the csv
'''

type = sys.argv[1]
t0 = time.time()

if type == 'csv':
        movies = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/movies.csv")
        movie_genres = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/movie_genres.csv")
        ratings = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/ratings.csv")
        
else:
        movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
        movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
        ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

movies.registerTempTable("movies")
movie_genres.registerTempTable("movie_genres")
ratings.registerTempTable("ratings")


sqlString = "SELECT m._c1 as movieGenre, AVG(r1.av_movie) as avgMovieRating, COUNT(*) as movieCount \
            FROM movie_genres m \
            JOIN \
                (SELECT r._c1 as movie,AVG(r._c2) as av_movie FROM ratings r GROUP BY r._c1) as r1 \
            ON r1.movie = m._c0 \
            GROUP BY m._c1 \
            ORDER BY movieGenre"


res = spark.sql(sqlString)
# res.show()

if type == 'csv':
        res.write.csv("hdfs://master:9000/outputs/part-1/sql/csv/q3sql-csv.csv")
else:
        res.write.csv("hdfs://master:9000/outputs/part-1/sql/pqt/q3sql-pqt.csv")

t1 = time.time()
t = t1 - t0

print("~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("query ran in %.4f sec  |"%(t))
print("~~~~~~~~~~~~~~~~~~~~~~~~~~")