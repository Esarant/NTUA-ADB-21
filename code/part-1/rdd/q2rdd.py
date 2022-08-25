from pyspark.sql import SparkSession
import csv, time
   

spark = SparkSession.builder.appName("q2rdd").getOrCreate()
sc = spark.sparkContext

t0 = time.time()
# map(user_id, (rating, counter-1))
# reduceByKey (user_id, (sum_of_ratings, counter_of_ratings))
# map(user_id, (avg_rating_in_all_movies))
# find average movie rating for each user

users = sc.textFile("hdfs://master:9000/files/ratings.csv"). \
    map(lambda x : (x.split(",")[0], (float(x.split(",")[2]), 1))). \
    reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])). \
    map(lambda x : (x[0], x[1][0]/x[1][1]))

kek = users.filter(lambda x: x[1] > 3).count()

#get user count by number of records on users variable
n_users = users.count()
#get total number of users that rated above 3
total_users = users.filter(lambda x: x[1] > 3).count()
percentage = str((total_users/n_users)*100)

plist = [{"percentage = ": percentage}]
df = spark.createDataFrame(plist)

#df.rdd.saveAsTextFile("hdfs://master:9000/outputs/part-1/rdd/q2rdd-out.csv")

t1 = time.time()
t = t1 - t0

print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
print(" The percentage is: ", percentage, "%")

print("~~~~~~~~~~~~~~~~~~~~~~~~~~")
print(" Query ran in %.4f sec  |"%(t))
print("~~~~~~~~~~~~~~~~~~~~~~~~~~")