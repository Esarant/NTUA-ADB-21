from pyspark.sql import SparkSession
import sys, time

spark = SparkSession.builder.appName("q4sql").getOrCreate()

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
        
else:
        movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
        movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")

movies.registerTempTable("movies")
movie_genres.registerTempTable("movie_genres")

def period5y(s):
  if s > 1999 and s < 2005:
    res = '2000-2004'
  elif s > 2004 and s < 2010:
    res = '2005-2009'
  elif s > 2009 and s < 2015:
    res = '2010-2014'
  elif s > 2014 and s < 2020:
    res = '2015-2019'
  else:
    res = '0'
  return res

spark.udf.register("resLen", lambda x : len(x.split(' ')))
spark.udf.register("period", period5y)


sqlString = \
        "SELECT re.period as Period, AVG(re.res_len) as AverageResumeLength FROM (\
        SELECT m._c0 as mid, resLen(m._c2) as res_len, period(YEAR(m._c3)) as period \
        FROM movies as m INNER JOIN movie_genres as g ON m._c0 = g._c0 \
        WHERE YEAR(m._c3) > 1999 AND g._c1 = 'Drama' AND m._c2 IS NOT NULL) as re \
        GROUP BY re.period \
        ORDER BY re.period"

res = spark.sql(sqlString)
#res.show()

if type == 'csv':
        res.write.csv("hdfs://master:9000/outputs/part-1/sql/csv/q4sql-csv.csv")
else:
        res.write.csv("hdfs://master:9000/outputs/part-1/sql/pqt/q4sql-pqt.csv")

t1 = time.time()
t = t1 - t0

print("~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("query ran in %.4f sec  |"%(t))
print("~~~~~~~~~~~~~~~~~~~~~~~~~~")