from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField
import codecs

#creating the lookup dictionary that is to be broadcasted
def bdcstSupList():
    SupDict={}
    with open(r"C:\Users\kshit\OneDrive\Documents\SparkCourse\16_MostPopularSuperHeroSparkSQL\Marvel-Names","r",encoding='ISO-8859-1',errors='ignore') as f:
        for line in f:
            fields=line.split(" ",1)
            SupDict[fields[0]]=fields[1]
    return SupDict

#Creating the Spark Session
spark=SparkSession.builder.appName('PopularSuperHero').getOrCreate()

#broadcasting the function to each executor
SupList=spark.sparkContext.broadcast(bdcstSupList())

#Ingesting the super hero connections data
SupSocNet=spark.read.text(r"C:\Users\kshit\OneDrive\Documents\SparkCourse\16_MostPopularSuperHeroSparkSQL\Marvel-Graph")

#creating the columns out of the unstructured data
SupSocdf=SupSocNet.withColumn("Sup ID",func.split(func.trim(func.col("value"))," ")[0]). \
                    withColumn("Sup Pals",func.size(func.split(func.trim(func.col("value"))," "))-1)

#aggregating the super hero connections
SupSocAgg=SupSocdf.groupBy("Sup ID").agg(func.sum("Sup Pals").alias("Connections"))

#creatig the function that is to be converted to UDF
def getSupName(supID):
    return SupList.value[supID]

#converting the function to UDF
SupNameUDF=func.udf(getSupName)

#Creating a new column that contains the Super Hero name
SupNameDf=SupSocAgg.withColumn("Sup Name",SupNameUDF(func.col("Sup ID"))).select(["Sup ID","Sup Name","Connections"])

#Sorting the final data on the descending order of the connections
SupNameSorted=SupNameDf.sort(func.desc("Connections"))

#collecting the results on the driver node
results=SupNameSorted.collect()

#printing the final results
for i in results[0:20]:
    print(str(i[1]).strip() +" is the most popular superhero with "+ str(i[2])+" connections. \n")


