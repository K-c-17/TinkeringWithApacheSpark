# Spark Notes

This document serves as a comprehensive guide to understanding and implementing Apache Spark, covering various components such as RDDs, DataFrames, Spark SQL, Spark Streaming, and Spark MLlib. It is designed to provide theoretical insights alongside practical examples to help readers master Spark.

---

## Table of Contents

- [Spark RDD Notes](#spark-rdd-notes)
- [Spark SQL Notes](#spark-sql-notes)
- [Machine Learning with Spark MLlib](#machine-learning-with-spark-mllib)
- [Spark Streaming](#spark-streaming)
- [Book Recommendations](#book-recommendations)

---

## Spark RDD Notes

1. **Entry Point:**
   - Prior to Spark 2.0: `SparkContext` was the entry point for interacting with the Spark cluster and RDDs.
   - From Spark 2.0 onwards: `SparkSession` became the unified entry point for working with DataFrames, Datasets, and SQL APIs. It simplifies the development process by integrating various functionalities into a single object.

2. **Key Transformations:**
   - `reduceByKey()`: Aggregates values by key, reducing data locally before shuffling across the cluster. Ideal for optimizing performance with large datasets.
   - `mapValues()`: Transforms values while preserving the original keys, making it useful for operations on key-value RDDs.
   - `groupByKey()`: Groups values by key. It can result in significant shuffling, so `reduceByKey()` is preferred when possible.

3. **Lambda Functions:**
   - `reduceByKey()` accepts two arguments, typically used to combine values.
   - `mapValues()` accepts one argument, focusing solely on value transformations.

4. **DataFrame Characteristics:**
   - Contains `Row` objects that can have schemas.
   - Efficient storage format with optimizations like Catalyst for query planning and Tungsten for execution.
   - Supports integration with various data formats (JSON, Hive, Parquet, CSV) and tools (JDBC/ODBC, Tableau).

5. **Creating Temporary Views:**
   - Temporary views allow SQL-like queries on DataFrames:
     ```python
     inputData.createOrReplaceTempView("myStructuredStuff")
     result = spark.sql("SELECT column FROM myStructuredStuff ORDER BY column")
     ```

6. **Common DataFrame Operations:**
   - Display data: `df.show()`
   - Select specific columns: `df.select("column")`
   - Filter rows: `df.filter(df("column") > 200)`
   - Perform aggregations: `df.groupBy("column").mean()`

7. **Using UDFs:**
   - User-defined functions enable custom transformations:
     ```python
     from pyspark.sql.types import IntegerType
     def square(x):
         return x * x
     spark.udf.register("square", square, IntegerType())
     result = spark.sql("SELECT square(column) FROM table")
     ```

8. **Ingesting Data:**
   - Load structured data:
     ```python
     df = spark.read.option("header", "true").option("inferSchema", "true").csv("file.csv")
     ```
   - Load unstructured data:
     ```python
     rdd = spark.sparkContext.textFile("file.txt")
     ```

9. **Caching and Persistence:**
   - Cache frequently used DataFrames or RDDs to memory:
     ```python
     df.cache()
     ```
   - Persist data to disk for fault tolerance:
     ```python
     df.persist(StorageLevel.MEMORY_AND_DISK)
     ```

10. **Partitioning:**
    - Optimize data distribution with `.partitionBy()` for large operations like joins or aggregations.
    - Use an appropriate number of partitions based on cluster resources to balance performance.

---

## Spark SQL Notes

1. **Sorting DataFrames:**
   - Sort data using `sort()` or `orderBy()`:
     ```python
     df.sort(col("column").asc()).show()
     df.orderBy(col("column").desc()).show()
     ```

2. **Useful Functions:**
   - `functions.explode()`: Converts arrays/maps into rows.
   - `functions.split()`: Splits strings into arrays based on delimiters.
   - `functions.lower()`: Converts text to lowercase for case-insensitive operations.

3. **Schema Definition:**
   - Define schemas for structured data:
     ```python
     from pyspark.sql.types import StructType, StructField, StringType, IntegerType
     schema = StructType([
         StructField("column1", StringType(), True),
         StructField("column2", IntegerType(), True)
     ])
     ```

4. **Adding Columns:**
   - Use `withColumn()` to modify or add columns:
     ```python
     df = df.withColumn("newColumn", col("existingColumn") * 2)
     ```

5. **Data Skew Analysis:**
   - Analyze partition distribution to identify and resolve skew:
     ```python
     import pyspark.sql.functions as F
     df.groupBy(F.spark_partition_id()).count().show()
     ```

---

## Machine Learning with Spark MLlib

1. **Key Features:**
   - Distributed machine learning algorithms for scalability.
   - Integration with Spark DataFrames and RDDs for seamless workflows.

2. **Linear Regression Example:**
   ```python
   from pyspark.ml.regression import LinearRegression
   lir = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
   model = lir.fit(trainingDF)
   predictions = model.transform(testDF)
   ```

3. **Decision Tree Example:**
   ```python
   from pyspark.ml.regression import DecisionTreeRegressor
   dtr = DecisionTreeRegressor(featuresCol="features", labelCol="label")
   model = dtr.fit(trainingDF)
   ```

4. **VectorAssembler:**
   - Combine columns into a single feature vector for ML models:
     ```python
     from pyspark.ml.feature import VectorAssembler
     assembler = VectorAssembler(inputCols=["col1", "col2"], outputCol="features")
     df = assembler.transform(data)
     ```

5. **Random Splits:**
   - Divide data into training and testing sets:
     ```python
     trainDF, testDF = df.randomSplit([0.8, 0.2])
     ```

---

## Spark Streaming

1. **Core Concepts:**
   - DStreams represent data streams as a sequence of RDDs.
   - Structured Streaming provides a high-level API for continuous data processing with DataFrames.

2. **Implementation:**
   - Example of Structured Streaming:
     ```python
     spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()
     lines = spark.readStream.text("logs")
     query = lines.writeStream.outputMode("append").format("console").start()
     query.awaitTermination()
     ```

3. **Windowed Operations:**
   - Perform aggregations over sliding time windows:
     ```python
     df.groupBy(func.window(col("timestamp"), "10 minutes", "5 minutes")).count()
     ```

4. **Checkpointing:**
   - Save state information to enable recovery from failures.

---

## Book Recommendations

1. **Learning Spark (O'Reilly):** Comprehensive guide to Spark fundamentals, including RDDs, DataFrames, and optimizations.
2. **Advanced Analytics with Spark (O'Reilly):** Focuses on implementing machine learning and data mining techniques in Spark.

---
