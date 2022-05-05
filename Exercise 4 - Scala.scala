// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession
 .builder
 .appName("SparkSQLExampleApp")
 .getOrCreate()

// Path to data set
val csvFile="/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

// Read and create a temporary view
// Infer schema (note that for larger files you may want to specify the schema)
val df = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .load(csvFile)

// Create a temporary view
df.createOrReplaceTempView("us_delay_flights_tbl")

// COMMAND ----------

spark.sql("""SELECT distance, origin, destination
FROM us_delay_flights_tbl WHERE distance > 1000
ORDER BY distance DESC""").show(10)

// COMMAND ----------

spark.sql("""SELECT date, delay, origin, destination
FROM us_delay_flights_tbl
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC""").show(10)

// COMMAND ----------

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

// COMMAND ----------

// spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
// MAGIC  SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
// MAGIC  origin = 'SFO';

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
// MAGIC  SELECT date, delay, origin, destination from us_delay_flights_tbl WHERE
// MAGIC  origin = 'JFK'

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM us_origin_airport_JFK_tmp_view

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view;
// MAGIC DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view

// COMMAND ----------

spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

// COMMAND ----------

spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")

// COMMAND ----------

val usFlightsDF = spark.sql("SELECT * FROM us_delay_flights_tbl")
val usFlightsDF2 = spark.table("us_delay_flights_tbl")

// COMMAND ----------

// Use Parquet
val file = """/databricks-datasets/learning-spark-v2/flights/summary-
 data/parquet/2010-summary.parquet"""
val df = spark.read.format("parquet").load(file)

// Use Parquet; you can omit format("parquet") if you wish as it's the default
val df2 = spark.read.load(file)

// Use CSV
val df3 = spark.read.format("csv")
 .option("inferSchema", "true")
 .option("header", "true")
 .option("mode", "PERMISSIVE")
 .load("/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*")

// Use JSON
val df4 = spark.read.format("json")
 .load("/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")

// COMMAND ----------

// val location = ...
df.write.format("json").mode("overwrite").save(location)

// COMMAND ----------

val file = """/databricks-datasets/learning-spark-v2/flights/summary-data/
 parquet/2010-summary.parquet/"""

val df = spark.read.format("parquet").load(file)

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// COMMAND ----------

df.write.format("parquet")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/parquet/df_parquet")

// COMMAND ----------

val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
val df = spark.read.format("json").load(file)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING json
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"
// MAGIC  )

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// COMMAND ----------

df.write.format("json")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/json/df_json")

// COMMAND ----------

val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"
val schema = "DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"
val df = spark.read.format("csv")
 .schema(schema)
 .option("header", "true")
 .option("mode", "FAILFAST") // Exit if any errors
 .option("nullValue", "") // Replace any null data with quotes
 .load(file)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING csv
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*",
// MAGIC  header "true",
// MAGIC  inferSchema "true",
// MAGIC  mode "FAILFAST"
// MAGIC  )

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show(10)

// COMMAND ----------

// Avro
val df = spark.read.format("avro")
.load("/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*")
df.show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW episode_tbl
// MAGIC  USING avro
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"
// MAGIC  )

// COMMAND ----------

spark.sql("SELECT * FROM episode_tbl").show(false)

// COMMAND ----------

df.write
 .format("avro")
 .mode("overwrite")
 .save("/tmp/data/avro/df_avro")

// COMMAND ----------

// ORC
val file = "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
val df = spark.read.format("orc").load(file)
df.show(10, false)

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl
// MAGIC  USING orc
// MAGIC  OPTIONS (
// MAGIC  path "/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"
// MAGIC  )

// COMMAND ----------

spark.sql("SELECT * FROM us_delay_flights_tbl").show()

// COMMAND ----------

df.write.format("orc")
 .mode("overwrite")
 .option("compression", "snappy")
 .save("/tmp/data/orc/df_orc")

// COMMAND ----------

import org.apache.spark.ml.source.image

val imageDir = "/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"
val imagesDF = spark.read.format("image").load(imageDir)

imagesDF.printSchema
imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode",
 "label").show(5, false)
