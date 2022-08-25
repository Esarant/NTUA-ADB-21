import csv
from pyspark.sql import SparkSession
#from pyspark.sql.functions import year
import sys, time
spark = SparkSession.builder.appName("q1sql").getOrCreate()

'''
 select file type between "csv" and "parquet"
 $ spark-submit q1sql.py csv
 would read the csv
'''
type = sys.argv[1]

if type == 'csv':
        movies = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/movies.csv")
else:
        movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")

movies.registerTempTable("movies")

sqlString = \
        "SELECT YEAR(_c3) as Year, (_c1) as MovieName, maximum as Profit\
        FROM \
        (SELECT *, MAX(100*(_c6-_c5)/(_c5)) \
        OVER (PARTITION BY YEAR(_c3)) \
        AS maximum \
        FROM movies) M \
        WHERE _c6 != 0 AND _c5 != 0 AND (100*(_c6-_c5)/(_c5)) = maximum AND YEAR(_c3) >= 2000\
        ORDER BY YEAR(_c3) ASC"

res = spark.sql(sqlString)

t0 = time.time()

if type == 'csv':
        res.write.csv("hdfs://master:9000/outputs/part-1/sql/csv/q1sql-csv.csv")
else:
        res.write.csv("hdfs://master:9000/outputs/part-1/sql/pqt/q1sql-pqt.csv")
#res.show()

t1 = time.time()
t = t1 - t0

print("~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("query ran in %.4f sec  |"%(t))
print("~~~~~~~~~~~~~~~~~~~~~~~~~~")




