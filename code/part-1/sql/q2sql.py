from pyspark.sql import SparkSession
import sys, time
spark = SparkSession.builder.appName("q2sql").getOrCreate()

'''
 select file type between "csv" and "parquet"
 $ spark-submit q1sql.py csv
 would read the csv
'''
type = sys.argv[1]

if type == 'csv':
        ratings = spark.read.option("header","false").option("delimiter",",").option("inferSchema","true").csv("hdfs://master:9000/files/ratings.csv")
else:
        ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

ratings.registerTempTable("ratings")

sqlString = '''
			SELECT COUNT(*)/( SELECT COUNT(*) 
			FROM (SELECT _c0 as ID, AVG(_c2) AS avg_rating
			FROM ratings
			GROUP BY _c0))*100 AS percentage
			FROM (SELECT _c0 AS id, AVG(_c2) AS avg_rating
			FROM ratings
			GROUP BY _c0) M
			WHERE M.avg_rating > 3
			'''

percent = spark.sql(sqlString)


t0 = time.time()

if type == 'csv':
        percent.write.csv("hdfs://master:9000/outputs/part-1/sql/csv/q2sql-csv.csv")
else:
        percent.write.csv("hdfs://master:9000/outputs/part-1/sql/pqt/q2sql-pqt.csv")


t1 = time.time()
t = t1 - t0

print("~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("query ran in %.4f sec |"%(t))
print("~~~~~~~~~~~~~~~~~~~~~~~~~~")

