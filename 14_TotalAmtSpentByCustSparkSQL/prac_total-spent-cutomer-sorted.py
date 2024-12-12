from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
spark=SparkSession.builder.appName("Total Spent By Customer").getOrCreate()

#defining the schema for the unstructured data
schema=StructType(  \
                [StructField("CustomerID",IntegerType(),True),  \
                 StructField("OrderID",IntegerType(),True),  \
                 StructField("OrderValue",FloatType(),True)])

#Ingesting the customer data with a pre-defined schema
df=spark.read.schema(schema).csv(r"C:\Users\kshit\OneDrive\Documents\SparkCourse\14_TotalAmtSpentByCustSparkSQL\customer-orders.csv")

#taking the subset of the df
df_sub=df.select("CustomerID","OrderValue")

#Aggregating the total amount by Customer ID
TotalAmtByCust=df_sub.groupBy("CustomerID").agg(func.round(func.sum("OrderValue"),2).alias("Total Amount"))

#sorting the total value data frame by Total Amount in desc order
TotalAmtSorted=TotalAmtByCust.sort(func.col("Total Amount").desc())

#Collecting the result on the driver node
results=TotalAmtSorted.collect()

#printing the final results
for result in results:
    print(str(result[0])+" has spent a total of " +str(result[1]))

spark.stop()



