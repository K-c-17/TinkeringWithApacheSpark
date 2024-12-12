from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as func
from pyspark.sql.functions import col

spark=SparkSession.builder.appName("FriendsByAge").getOrCreate()

#Reading the structured data
friends=spark.read.option('header','true').option('inferSchema','true').csv(r"C:\Users\kshit\OneDrive\Documents\SparkCourse\11_FriendsByAgeSparkSQL_2\fakefriends-header.csv")

#taking a subset of the friends df that includes only the age and friends columns
subFriends=friends.select('age','friends') 

#Grouping Friends by Age and sroting by average number of friends
friendsByAge=subFriends.groupBy("age").agg(func.round(func.avg("friends"),2).alias('Average Friends')).sort(col("Average Friends").asc())

#showing the final results
friendsByAge.show()

#stopping the spark session
spark.stop()