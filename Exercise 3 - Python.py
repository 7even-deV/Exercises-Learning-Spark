# Databricks notebook source
# Create an RDD of tuples (name, age)

dataRDD = sc.parallelize([("Brooke", 20), ("Denny", 31), ("Jules", 30), ("TD", 35), ("Brooke", 25)])
# Use map and reduceByKey transformations with their lambda
# expressions to aggregate and then compute average
agesRDD = (dataRDD
    .map(lambda x: (x[0], (x[1], 1)))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
    .map(lambda x: (x[0], x[1][0]/x[1][1])))

# COMMAND ----------

# In Python
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Create a DataFrame using SparkSession
spark = (SparkSession
    .builder
    .appName("AuthorsAges")
    .getOrCreate())

# Create a DataFrame
data_df = spark.createDataFrame([("Brooke", 20), ("Denny", 31), ("Jules", 30),
                                ("TD", 35), ("Brooke", 25)], ["name", "age"])

# Group the same names together, aggregate their ages, and compute an average
avg_df = data_df.groupBy("name").agg(avg("age"))

# Show the results of the final execution
avg_df.show()


# COMMAND ----------

# Two ways to define a schema

from pyspark.sql.types import *
schema = StructType([StructField("author", StringType(), False),
    StructField("title", StringType(), False),
    StructField("pages", IntegerType(), False)])

schema = "author STRING, title STRING, pages INT"

# COMMAND ----------

# You can choose whichever way you like to define a schema. For many examples, we
# will use both:
from pyspark.sql import SparkSession

# Define schema for our data using DDL
schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING,`Published` STRING, `Hits` INT, `Campaigns` ARRAY<STRING>"

# Create our static data
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter",
"LinkedIn"]],
 [2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter",
"LinkedIn"]],
 [3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web",
"twitter", "FB", "LinkedIn"]],
 [4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,
["twitter", "FB"]],
 [5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web",
"twitter", "FB", "LinkedIn"]],
 [6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,
["twitter", "LinkedIn"]]
 ]

# Main program
if __name__ == "__main__":
    # Create a SparkSession
    spark = (SparkSession
        .builder
        .appName("Example-3_6")
        .getOrCreate())
    
    # Create a DataFrame using the schema defined above
    blogs_df = spark.createDataFrame(data, schema)
    
    # Show the DataFrame; it should reflect our table above
    blogs_df.show()
    
    # Print the schema used by Spark to process the DataFrame
    print(blogs_df.printSchema())

# COMMAND ----------

from pyspark.sql import Row
blog_row = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
 ["twitter", "LinkedIn"])

# access using index for individual items
blog_row[1]

# COMMAND ----------

rows = [Row("Matei Zaharia", "CA"), Row("Reynold Xin", "CA")]
authors_df = spark.createDataFrame(rows, ["Authors", "State"])
authors_df.show()

# COMMAND ----------

from pyspark.sql.types import *
# Programmatic way to define a schema

fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
    StructField('UnitID', StringType(), True),
    StructField('IncidentNumber', IntegerType(), True),
    StructField('CallType', StringType(), True),
    StructField('CallDate', StringType(), True),
    StructField('WatchDate', StringType(), True),
    StructField('CallFinalDisposition', StringType(), True),
    StructField('AvailableDtTm', StringType(), True),
    StructField('Address', StringType(), True),
    StructField('City', StringType(), True),
    StructField('Zipcode', IntegerType(), True),
    StructField('Battalion', StringType(), True),
    StructField('StationArea', StringType(), True),
    StructField('Box', StringType(), True),
    StructField('OriginalPriority', StringType(), True),
    StructField('Priority', StringType(), True),
    StructField('FinalPriority', IntegerType(), True),
    StructField('ALSUnit', BooleanType(), True),
    StructField('CallTypeGroup', StringType(), True),
    StructField('NumAlarms', IntegerType(), True),
    StructField('UnitType', StringType(), True),
    StructField('UnitSequenceInCallDispatch', IntegerType(), True),
    StructField('FirePreventionDistrict', StringType(), True),
    StructField('SupervisorDistrict', StringType(), True),
    StructField('Neighborhood', StringType(), True),
    StructField('Location', StringType(), True),
    StructField('RowID', StringType(), True),
    StructField('Delay', FloatType(), True)])

# Use the DataFrameReader interface to read a CSV file
sf_fire_file = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema)

# COMMAND ----------

fire_df.cache()
fire_df.count()
fire_df.printSchema()
display(fire_df.limit(5))

# COMMAND ----------

few_fire_df = (fire_df
    .select("IncidentNumber", "AvailableDtTm", "CallType")
    .where(col("CallType") != "Medical Incident"))

few_fire_df.show(5, truncate=False)

# COMMAND ----------

# In Python, return number of distinct types of calls using countDistinct()
from pyspark.sql.functions import *
(fire_df

 .select("CallType")
 .where(col("CallType").isNotNull())
 .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 .show(10))

# COMMAND ----------

new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
    .select("ResponseDelayedinMins")
    .where(col("ResponseDelayedinMins") > 5)
    .show(5, False))

# COMMAND ----------

fire_ts_df = (new_fire_df
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
 "MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm"))

# Select the converted columns
(fire_ts_df
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, False))

# COMMAND ----------

(fire_ts_df
 .select(year('IncidentDate'))
 .distinct()
 .orderBy(year('IncidentDate'))
 .show())

# COMMAND ----------

(fire_ts_df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .groupBy("CallType")
 .count()
 .orderBy("count", ascending=False)
 .show(n=10, truncate=False))

# COMMAND ----------

import pyspark.sql.functions as F
(fire_ts_df
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
 F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
 .show())

# COMMAND ----------

# ¿Cuáles fueron todos los diferentes tipos de llamadas de emergencia en 2018?
fire_ts_df.select(year("IncidentDate")).distinct().orderBy(year("IncidentDate")).show()

# COMMAND ----------

# ¿Qué meses dentro del año 2018 vieron la mayor cantidad de llamadas de emergencia?
fire_ts_df.filter(year("IncidentDate") == 2018).groupBy(weekofyear("IncidentDate")).count().orderBy(desc("count")).show()

# COMMAND ----------

# ¿Qué vecindario en San Francisco generó la mayor cantidad de llamadas de emergencia en 2018?
fire_ts_df.select("Neighborhood", "ResponseDelayedinMins").filter(year("IncidentDate") == 2018).show(10, False)

# COMMAND ----------

# ¿Qué vecindarios tuvieron los peores tiempos de respuesta a las llamadas de incendios en 2018?
fire_ts_df.write.format("parquet").mode("overwrite").save("/tmp/fireServiceParquet/")

# COMMAND ----------

# ¿Qué semana del año en 2018 tuvo la mayor cantidad de llamadas de emergencia?
fire_ts_df.write.format("parquet").mode("overwrite").saveAsTable("FireServiceCalls")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿Existe una correlación entre el vecindario, el código postal y el número de llamadas de emergencia?
# MAGIC CACHE TABLE FireServiceCalls

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ¿Cómo podemos usar archivos Parquet o tablas SQL para almacenar estos datos y leerlos?
# MAGIC SELECT * FROM FireServiceCalls LIMIT 10

# COMMAND ----------

from pyspark.sql import Row
row = Row(350, True, "Learning Spark 2E", None)
row[0]
row[1]
row[2]

# COMMAND ----------

count_mnm_df = (mnm_df
 .select("State", "Color", "Count")
 .groupBy("State", "Color")
 .agg(count("Count")
 .alias("Total"))
 .orderBy("Total", ascending=False))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT State, Color, Count, sum(Count) AS Total
# MAGIC FROM MNM_TABLE_NAME
# MAGIC GROUP BY State, Color, Count
# MAGIC ORDER BY Total DESC

# COMMAND ----------

# Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por defecto.
from pyspark.sql.functions import *
from pyspark.sql.types import *

mnm_file = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

mnm_df = (spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(mnm_file))

mnm_df.printSchema()
