from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("csv2parquet").getOrCreate()


#defining the input & output paths

in_movie_genres = "hdfs://master:9000/files/movie_genres.csv"
out_movie_genres = "hdfs://master:9000/files/movie_genres.parquet"

in_movies = "hdfs://master:9000/files/movies.csv"
out_movies = "hdfs://master:9000/files/movies.parquet"

in_ratings = "hdfs://master:9000/files/ratings.csv"
out_ratings = "hdfs://master:9000/files/ratings.parquet"

#read & write for movie_genres
# read csv to dataframe
df = spark.read.format("csv").options(header='false', inferSchema='true').load(in_movie_genres)
# write as parquet
df.write.parquet(out_movie_genres)

# same for movies
df = spark.read.format("csv").options(header='false', inferSchema='true').load(in_movies)
df.write.parquet(out_movies)

# same for ratings
df = spark.read.format("csv").options(header='false', inferSchema='true').load(in_ratings)
df.write.parquet(out_ratings)
