'''
Problem Statement:  Modify the StructuredStreaming Script to keep track
                    of the most-viewed URL's (endpoints) in our logs.
                    Also, use a 30 sec window and a 10 sec slide.

'''

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.functions import regexp_extract
from pyspark.sql import functions as func

#Initializing the spark Session
spark=SparkSession.builder.appName("Top URLs").getOrCreate()

streamDf=spark.readStream.text(r"C:\Users\kshit\OneDrive\Documents\SparkCourse\25_TopURLInLast30SecProblem\logs")

# Parse out the common log format to a DataFrame
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'

#Parsing out the relevant info (timestamp and api endpoint) into a dataframe
logsDF=streamDf.select( \
                        regexp_extract('value',timeExp,1).alias("timestamp"),       \
                        regexp_extract('value',generalExp,2).alias("endpoint")      \
                        )
#creating an additional column in the datastream so as to keep a track of the time with the incoming block of data and decide the window interval
#We can't use the existing timestamp column in the log as these logs are old and the timestamp as of a past time
logsDF=logsDF.withColumn("eventTime",func.current_timestamp())

#Keep running the score of the count of the API endpoint across a 30 sec sliding window with 10 sec slide
statusCountDF=logsDF.groupBy(func.window(func.col("eventTime"),"30 seconds","10 seconds"),func.col("endpoint")).count()

#Ordering the results by descending order of the counts
finalDf=statusCountDF.orderBy(func.col("count").desc())

#Kick off the streaming query and output the results to console
query=  (finalDf.writeStream.outputMode("complete").format("console").queryName("counts").start())

#Keep running the query until terminated
query.awaitTermination()

#Close the spark session 
spark.stop()