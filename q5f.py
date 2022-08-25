from pyspark.sql import SparkSession
import sys
spark = SparkSession.builder.appName("Query5-SparkSQL").getOrCreate()

# The user must give the input format (csv || parquet)

# For example:
# spark-submit SparkSqlquery5.py csv
# to read csv file

input_format = sys.argv[1]

m = ( "id, title, resume, date, length, cost, revenue, score" )
#      c0    c1     c2     c3      c4    c5      c6      c7

r = (" uid, mid, rating, date ")
#       c0   c1   c2       c3

g = (" mid, genre ")
#       c0    c1

sqlString = \
        "select b.Genre, first(b.UserId) as UserId, first(b.SumOfRatings) as SumOfRatings, first(b.Name) as FavoriteName, first(b.Rating) as FavoriteRating, \
                first(b.WorstName) as WorstName, first(b.WorstRating) as WorstRating\
        from\
                (select a.Genre, a.UserId, a.WorstRating, a.SumOfRatings, a.Name, a.Rating, a.FavRating, max(SumOfRatings) over (PARTITION BY Genre) as MaxSum, a.FavName, a.WorstName\
                from\
                        (select g._c1 as Genre, r._c0 as UserId, m._c1 as Name, r._c2 as Rating,\
                                count(r._c2) over (PARTITION BY g._c1, r._c0) as SumOfRatings, \
                                first(r._c2) over (PARTITION BY g._c1, r._c0 order by r._c2 DESC, m._c7 DESC) as FavRating,\
                                first(m._c1) over (PARTITION BY g._c1, r._c0 order by r._c2 DESC, m._c7 DESC) as FavName, \
                                first(r._c2) over (PARTITION BY g._c1, r._c0 order by r._c2 ASC, m._c7 DESC) as WorstRating,\
                                first(m._c1) over (PARTITION BY g._c1, r._c0 order by r._c2 ASC, m._c7 DESC) as WorstName\
                        from movie_genres as g inner join ratings as r on g._c0 = r._c1 inner join movies as m on m._c0 = r._c1\
                        )a\
                )b\
        where b.SumOfRatings = b.MaxSum and b.FavRating = b.Rating\
        group by b.Genre\
        order by b.Genre ASC"


res = spark.sql(sqlString)
res.show(26)
