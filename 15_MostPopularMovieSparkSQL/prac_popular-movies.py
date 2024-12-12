from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,LongType,StringType
import codecs

#creating the lookup dictionary that is to be broadcasted
def MovieLookUp():
    MovieDict={}
    with codecs.open(r"C:\Users\kshit\OneDrive\Documents\SparkCourse\15_MostPopularMovieSparkSQL\u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields=line.split('|')
            MovieDict[int(fields[0])]=fields[1]
    return MovieDict

#Creating the Spark Session
spark=SparkSession.builder.appName("MostPopularMovies").getOrCreate()

#broadcasting the function to each executor
movieBdct=spark.sparkContext.broadcast(MovieLookUp())

#defining schema
schema=StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])

#ingesting the rating data
ratings=spark.read.option("sep","\t").schema(schema).csv(r"C:\Users\kshit\OneDrive\Documents\SparkCourse\15_MostPopularMovieSparkSQL\u.data")

#subsetting the relevant data
ratings1=ratings.select("movieID","rating")

#aggregating the data based on count
ratingCount=ratings1.groupBy("movieID").count()

#defining the function that is to be converted to UDF
def getMovieName(movieid):
    return movieBdct.value[movieid]

#UDF conversion of the function
lookupMovieUDF=func.udf(getMovieName)

#Creating new column tha contains the Movie Names
ratingImprvd=ratingCount.withColumn("MovieName",lookupMovieUDF(func.col("movieID")))

#Sroting the final df by most popular movies
ratingSorted=ratingImprvd.sort(func.desc("count"))

#Showing the final data
ratingSorted.select("MovieID","MovieName").show(truncate=False)

spark.stop()
