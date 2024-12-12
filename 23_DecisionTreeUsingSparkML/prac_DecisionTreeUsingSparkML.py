'''
Problem Statement: Predicting the price per unit area based on house age,
distance to MRT (public transportation), and number of nearby covenience
stores.

Hint:   Use the VectorAssembler to assemble the input columns in the form of an vector in the modle
        Use DecisionTreeRegressor.
        DTR doesn't require scaling of the data
        Since the underlying dataset that we using has the header row - so no need
        to hard-code a schema for reading the data file

'''
 
from pyspark.sql import SparkSession
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.sql import functions as func
from pyspark.ml.feature import VectorAssembler

spark=SparkSession.builder.appName("DTRImplementation").getOrCreate()

#ingesting the data into a spark dataframe
houseDf=spark.read.option("inferSchema","true").option('header','true').csv("realestate.csv")

#Taking only the relevant values
houseDf=houseDf.select("HouseAge","DistanceToMRT","NumberConvenienceStores","PriceOfUnitArea")

#Creating the feature vector using the Vector Assembler
featureVector=VectorAssembler().setInputCols(["HouseAge","DistanceToMRT","NumberConvenienceStores"]).setOutputCol("features")

transformDf=featureVector.transform(houseDf).select("PriceOfUnitArea","features")

#spliting the data into training and testing dataset
splitdf=transformDf.randomSplit([0.5,0.5])
train=splitdf[0]
test=splitdf[1]

#initializing the DecisionTreeRegressor Model
model=DecisionTreeRegressor(featuresCol='features',labelCol='PriceOfUnitArea',maxDepth=5)

#Training the Model
DTRModel=model.fit(train)

#Using the Model on the test dataset
fullPredictionDf=DTRModel.transform(test)

#Formatting the final df containing the label and the predicted value
finalDf=fullPredictionDf.select("PriceOfUnitArea","prediction").withColumnRenamed("prediction","Predicted PriceOfUnitArea")

#Collecting the final results in driver and printing it
for results in finalDf.collect():
    print('Predicted value: '+str(round(results[1],2))+' Actual value: '+str(results[0]))


#closing the spark session
spark.stop()


