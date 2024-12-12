from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType

spark=SparkSession.builder.appName('MinTempProblem').getOrCreate()

#defining the schema
schema=StructType([ \
                    StructField("stationID",StringType(),True), \
                    StructField("date",IntegerType(),True), \
                    StructField("measure_type",StringType(),True), \
                    StructField("temperature",StringType(),True)])

#ingesting the unstructured data with the predefined schema
df=spark.read.schema(schema).csv(r"C:\Users\kshit\OneDrive\Documents\SparkCourse\13_MinTempByLocSparkSQL\1800.csv")

#checking the schema of the df
df.printSchema()

#filtering out the only TMIN rows
df_min=df.filter(df.measure_type=="TMIN")

#susetting the relevant column
df_sub=df_min.select("stationID","temperature")

AvgMinTemp=df_sub.groupBy("stationID").agg(func.min("temperature").alias("Min Temperature")).orderBy(func.col("Min Temperature").desc())

AvgMinTemp.show()

spark.stop() 