from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as func

spark=SparkSession.builder.appName('Word Count Problem').getOrCreate()


book=spark.read.text(r"C:\Users\kshit\OneDrive\Documents\SparkCourse\12_WordCountSparkSQL\book.txt")

#Splitting the string row objects into an array and then exploding that array into multiple rows
word=book.select(func.explode(func.split(book.value,'\\W+')).alias("Words"))

#fitlering out any blank words
word.filter(word.Words!="")

#converting the words into lowercase
lowercaseWords=word.select(func.lower(func.col("Words")).alias("Words"))

#Calculating the count of the words in the word dataframe
wordCount=word.groupBy("Words").agg(func.count("Words").alias("WordCount"))

#Sorting the results by counts in descending order
sortedCount=wordCount.sort(func.col("WordCount").desc())

#showing the final results
sortedCount.show()