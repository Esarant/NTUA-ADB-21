from pyspark.sql import SparkSession
from io import StringIO
import csv, time

spark = SparkSession.builder.appName("q1rdd").getOrCreate()

sc = spark.sparkContext

def split_complex(x):
        return list(csv.reader(StringIO(x), delimiter=','))[0]

t0 = time.time()

result = \
        sc.textFile("hdfs://master:9000/files/movies.csv"). \
	map(lambda x : (split_complex(x)[3][0:4], split_complex(x)[0], split_complex(x)[1], int(split_complex(x)[5]), int(split_complex(x)[6]))). \
	filter(lambda x : x[0] != '' and x[3] != 0 and x[4] != 0 and int(x[0]) >= 2000). \
        map(lambda x : (x[0], (x[1], x[2], (x[4]-x[3])*100/x[3]))). \
        reduceByKey(lambda x,y: x if x[2]>y[2] else y). \
        sortByKey(). \
        map(lambda x : ','.join(str(i) for i in x))

# collect()

# print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
# print("( year , (movie_id, title, profit) )")
# for i in result:
#         print(i)

result.saveAsTextFile("hdfs://master:9000/outputs/part-1/rdd/q1rdd-out.csv")

t1 = time.time()
t = t1 - t0

print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
print("query ran in %.4f sec       |"%(t))
print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
