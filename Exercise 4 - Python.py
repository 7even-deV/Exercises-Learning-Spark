# Databricks notebook source
from pyspark.sql import SparkSession

# Create a SparkSession
spark = (SparkSession
 .builder
 .appName("SparkSQLExampleApp")
 .getOrCreate())

# Path to data set
csv_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

# Read and create a temporary view
# Infer schema (note that for larger files you
# may want to specify the schema)
df = (spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csv_file))

df.createOrReplaceTempView("us_delay_flights_tbl")

# COMMAND ----------

spark.sql("""SELECT distance, origin, destination
FROM us_delay_flights_tbl WHERE distance > 1000
ORDER BY distance DESC""").show(10)

# COMMAND ----------

spark.sql("""SELECT date, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC""").show(10)

# COMMAND ----------

spark.sql("""SELECT delay, origin, destination,
 CASE
 WHEN delay > 360 THEN 'Very Long Delays'
 WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
 WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
 WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
 WHEN delay = 0 THEN 'No Delays'
 ELSE 'Early'
 END AS Flight_Delays
 FROM us_delay_flights_tbl
 ORDER BY origin, delay DESC""").show(10)

# COMMAND ----------

from pyspark.sql.functions import col, desc
(df.select("distance", "origin", "destination")
 .where(col("distance") > 1000)
 .orderBy(desc("distance"))).show(10)

# COMMAND ----------

# Or
(df.select("distance", "origin", "destination")
 .where("distance > 1000")
 .orderBy("distance", ascending=False).show(10))

# COMMAND ----------

spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# COMMAND ----------

# You can do the same thing using the DataFrame API like this:
csv_file = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

# Schema as defined in the preceding example
schema="date STRING, delay INT, distance INT, origin STRING, destination STRING"
flights_df = spark.read.csv(csv_file, schema=schema)
flights_df.write.saveAsTable("managed_us_delay_flights_tbl")

# COMMAND ----------

spark.sql("""CREATE TABLE us_delay_flights_tbl(date STRING, delay INT,
 distance INT, origin STRING, destination STRING)
 USING csv OPTIONS (PATH
 '/databricks-datasets/learning-spark-v2/flights/departuredelays.csv')""")

# COMMAND ----------

# And within the DataFrame API use:
(flights_df
 .write
 .option("path", "/tmp/data/us_flights_delay")
 .saveAsTable("us_delay_flights_tbl"))


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
# MAGIC  SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
# MAGIC  origin = 'SFO';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
# MAGIC  SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
# MAGIC  origin = 'JFK'

# COMMAND ----------

df_sfo = spark.sql("SELECT date, delay, origin, destination FROM
 us_delay_flights_tbl WHERE origin = 'SFO'")
                   
df_jfk = spark.sql("SELECT date, delay, origin, destination FROM
 us_delay_flights_tbl WHERE origin = 'JFK'")
                   
Create a temporary and global temporary view
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view;
# MAGIC DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view

# COMMAND ----------

spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

# COMMAND ----------

spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")

# COMMAND ----------

us_flights_df = spark.sql("SELECT * FROM us_delay_flights_tbl")
us_flights_df2 = spark.table("us_delay_flights_tbl")

# COMMAND ----------

file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/
 2010-summary.parquet/"""
df = spark.read.format("parquet").load(file)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC  USING parquet
# MAGIC  OPTIONS (
# MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/
# MAGIC  2010-summary.parquet/" )

# COMMAND ----------

file = """/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/
 2010-summary.parquet/"""
df = spark.read.format("parquet").load(file)

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# COMMAND ----------

(df.write.format("parquet")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/parquet/df_parquet"))

# COMMAND ----------

file = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
df = spark.read.format("json").load(file)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC  USING json
# MAGIC  OPTIONS (
# MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
# MAGIC  )

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# COMMAND ----------

(df.write.format("json")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/json/df_json"))

# COMMAND ----------

file = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
df = (spark.read.format("csv")
 .option("header", "true")
 .schema(schema)
 .option("mode", "FAILFAST") # Salir si hay algún error
 .option("nullValue", "") # Reemplace cualquier campo de datos nulos con comillas
 .load(file))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC  USING csv
# MAGIC  OPTIONS (
# MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*",
# MAGIC  header "true",
# MAGIC  inferSchema "true",
# MAGIC  mode "FAILFAST"
# MAGIC  )

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10)

# COMMAND ----------

# Avro
df = (spark.read.format("avro")
 .load("/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"))
df.show(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW episode_tbl
# MAGIC  USING avro
# MAGIC  OPTIONS (
# MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
# MAGIC  )

# COMMAND ----------

spark.sql("SELECT * FROM episode_tbl").show(truncate=False)

# COMMAND ----------

(df.write
 .format("avro")
 .mode("overwrite")
 .save("/tmp/data/avro/df_avro"))

# COMMAND ----------

# ORC
file = "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
df = spark.read.format("orc").option("path", file).load()
df.show(10, False)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
# MAGIC  USING orc
# MAGIC  OPTIONS (
# MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
# MAGIC  )

# COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()

# COMMAND ----------

(df.write.format("orc")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/orc/flights_orc"))

# COMMAND ----------

from pyspark.ml import image

image_dir = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
images_df = spark.read.format("image").load(image_dir)
images_df.printSchema()

# COMMAND ----------

images_df.select("image.height", "image.width", "image.nChannels", "image.mode",
 "label").show(5, truncate=False)

# COMMAND ----------

path = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
binary_files_df = (spark.read.format("binaryFile")
 .option("pathGlobFilter", "*.jpg")
 .load(path))
binary_files_df.show(5)

# COMMAND ----------

binary_files_df = (spark.read.format("binaryFile")
 .option("pathGlobFilter", "*.jpg")
 .option("recursiveFileLookup", "true")
 .load(path))
binary_files_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC GlobalTempView vs TempView

# COMMAND ----------

# GlobalTempView: Crea o reemplaza una vista temporal global usando el nombre dado.
# La vida útil de esta vista temporal está vinculada a esta aplicación Spark.

# TempView: Crea o reemplaza una vista temporal local con este DataFrame.
# La duración de esta tabla temporal está vinculada a la SparkSession que se usó para crear este DataFrame.
