// Databricks notebook source
// In Scala
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.SparkSession

// Create a DataFrame using SparkSession
val spark = SparkSession
  .builder
  .appName("AuthorsAges")
  .getOrCreate()

// Create a DataFrame of names and ages
val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25), ("Denny", 31),
                                       ("Jules", 30), ("TD", 35))).toDF("name", "age")

// Group the same names together, aggregate their ages, and compute an average
val avgDF = dataDF.groupBy("name").agg(avg("age"))

// Show the results of the final execution
avgDF.show()

// COMMAND ----------

// If you were to read the data from a JSON file instead of creating static data, the
// schema definition would be identical. Let’s illustrate the same code with a Scala
// example, this time reading from a JSON file:
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


// Get the path to the JSON file
val jsonFile = "/FileStore/tables/blogs.json"

// Define our schema programmatically
val schema = StructType(Array(StructField("Id", IntegerType, false),
    StructField("First", StringType, false),
    StructField("Last", StringType, false),
    StructField("Url", StringType, false),
    StructField("Published", StringType, false),
    StructField("Hits", IntegerType, false),
    StructField("Campaigns", ArrayType(StringType), false)))

// Create a DataFrame by reading from the JSON file
// with a predefined schema
val blogsDF = spark.read.schema(schema).json(jsonFile)

// Show the DataFrame schema as output
blogsDF.show(false)

// Print the schema
println(blogsDF.printSchema)
println(blogsDF.schema)

// COMMAND ----------

import org.apache.spark.sql.functions._
blogsDF.columns

// Access a particular column with col and it returns a Column type
blogsDF.col("Id")

// Use an expression to compute a value
blogsDF.select(expr("Hits * 2")).show(2)

// or use col to compute value
blogsDF.select(col("Hits") * 2).show(2)

// COMMAND ----------

blogsDF.withColumn("Big Hitters", (expr("Hits > 10000"))).show()

// COMMAND ----------

// Concatenate three columns, create a new column, and show the
// newly created concatenated column
blogsDF
  .withColumn("AuthorsId", (concat(expr("First"), expr("Last"), expr("Id"))))
  .select(col("AuthorsId"))
  .show(4)

// COMMAND ----------

// These statements return the same value, showing that
// expr is the same as a col method call
blogsDF.select(expr("Hits")).show(2)
blogsDF.select(col("Hits")).show(2)
blogsDF.select("Hits").show(2)

// COMMAND ----------

// Sort by column "Id" in descending order
blogsDF.sort(col("Id").desc).show()
blogsDF.sort($"Id".desc).show()


// COMMAND ----------

import org.apache.spark.sql.Row

// Create a Row
val blogRow = Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015",
    Array("twitter", "LinkedIn"))

// Access using index for individual items
blogRow(1)

// COMMAND ----------

val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
val authorsDF = rows.toDF("Author", "State")
authorsDF.show()

// COMMAND ----------

val sampleDF = spark
  .read
  .option("samplingRatio", 0.001)
  .option("header", true)
  .csv("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

// COMMAND ----------

val fireSchema = StructType(Array(
  StructField("CallNumber", IntegerType, true),
  StructField("UnitID", StringType, true),
  StructField("IncidentNumber", IntegerType, true),
  StructField("CallType", StringType, true),                  
  StructField("CallDate", StringType, true),      
  StructField("WatchDate", StringType, true),
  StructField("CallFinalDisposition", StringType, true),
  StructField("AvailableDtTm", StringType, true),
  StructField("Address", StringType, true),       
  StructField("City", StringType, true),       
  StructField("Zipcode", IntegerType, true),       
  StructField("Battalion", StringType, true),                 
  StructField("StationArea", StringType, true),       
  StructField("Box", StringType, true),       
  StructField("OriginalPriority", StringType, true),       
  StructField("Priority", StringType, true),       
  StructField("FinalPriority", IntegerType, true),       
  StructField("ALSUnit", BooleanType, true),       
  StructField("CallTypeGroup", StringType, true),
  StructField("NumAlarms", IntegerType, true),
  StructField("UnitType", StringType, true),
  StructField("UnitSequenceInCallDispatch", IntegerType, true),
  StructField("FirePreventionDistrict", StringType, true),
  StructField("SupervisorDistrict", StringType, true),
  StructField("Neighborhood", StringType, true),
  StructField("Location", StringType, true),
  StructField("RowID", StringType, true),
  StructField("Delay", FloatType, true)))

val fireDF = spark
  .read
  .schema(fireSchema)
  .option("header", "true")
  .csv("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

// COMMAND ----------

val fewFireDF = fireDF
  .select("IncidentNumber", "AvailableDtTm", "CallType") 
  .where($"CallType" =!= "Medical Incident")

fewFireDF.show(5, false)

// COMMAND ----------

import org.apache.spark.sql.functions._
fireDF
 .select("CallType")
 .where(col("CallType").isNotNull)
 .agg(countDistinct('CallType) as 'DistinctCallTypes)
 .show(10)

// COMMAND ----------

val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedinMins")
newFireDF
 .select("ResponseDelayedinMins")
 .where($"ResponseDelayedinMins" > 5)
 .show(5, false)

// COMMAND ----------

val fireTsDF = newFireDF
 .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
 .drop("CallDate")
 .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
 .drop("WatchDate")
 .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
 "MM/dd/yyyy hh:mm:ss a"))
 .drop("AvailableDtTm")

// Select the converted columns
fireTsDF
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, false)

// COMMAND ----------

fireTsDF
 .select(year($"IncidentDate"))
 .distinct()
 .orderBy(year($"IncidentDate"))
 .show()

// COMMAND ----------

fireTsDF
 .select("CallType")
 .where(col("CallType").isNotNull)
 .groupBy("CallType")
 .count()
 .orderBy(desc("count"))
 .show(10, false)

// COMMAND ----------

import org.apache.spark.sql.{functions => F}
fireTsDF
 .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
 F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
 .show()

// COMMAND ----------

// ¿Cuáles fueron todos los diferentes tipos de llamadas de emergencia en 2018?
fireTsDF.select(year($"IncidentDate")).distinct().orderBy(year($"IncidentDate")).show()

// COMMAND ----------

// ¿Qué meses dentro del año 2018 vieron la mayor cantidad de llamadas de emergencia?
fireTsDF.filter(year($"IncidentDate") === 2018).groupBy(weekofyear($"IncidentDate")).count().orderBy(desc("count")).show()

// COMMAND ----------

// ¿Qué vecindario en San Francisco generó la mayor cantidad de llamadas de emergencia en 2018?
fireTsDF.select("Neighborhood", "ResponseDelayedinMins").filter(year($"IncidentDate") === 2018).show(10, false)

// COMMAND ----------

// ¿Qué vecindarios tuvieron los peores tiempos de respuesta a las llamadas de incendios en 2018?
fireTsDF.write.format("parquet").mode("overwrite").save("/tmp/fireServiceParquet/")

// COMMAND ----------

// ¿Qué semana del año en 2018 tuvo la mayor cantidad de llamadas de emergencia?
fireTsDF.write.format("parquet").mode("overwrite").saveAsTable("FireServiceCalls")

// COMMAND ----------

// MAGIC %sql
// MAGIC -- ¿Existe una correlación entre el vecindario, el código postal y el número de llamadas de emergencia?
// MAGIC CACHE TABLE FireServiceCalls

// COMMAND ----------

// MAGIC %sql
// MAGIC -- ¿Cómo podemos usar archivos Parquet o tablas SQL para almacenar estos datos y leerlos?
// MAGIC SELECT * FROM FireServiceCalls LIMIT 10

// COMMAND ----------

import org.apache.spark.sql.Row
val row = Row(350, true, "Learning Spark 2E", null)

row.getInt(0)
row.getBoolean(1)
row.getString(2)

// COMMAND ----------

case class DeviceIoTData (battery_level: Long, c02_level: Long,
cca2: String, cca3: String, cn: String, device_id: Long,
device_name: String, humidity: Long, ip: String, latitude: Double,
lcd: String, longitude: Double, scale:String, temp: Long,
timestamp: Long)

// COMMAND ----------

val ds = (spark.read
  .json("/databricks-datasets/learning-spark-v2/iot-devices/iot_devices.json")
  .as[DeviceIoTData])

ds.show(5, false)

// COMMAND ----------

val filterTempDS = ds.filter(d => {d.temp > 30 && d.humidity > 70})
filterTempDS.show(5, false)

// COMMAND ----------

case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long,
   cca3: String)
val dsTemp = ds
   .filter(d => {d.temp > 25})
   .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
   .toDF("temp", "device_name", "device_id", "cca3")
   .as[DeviceTempByCountry]
dsTemp.show(5, false)

// COMMAND ----------

val device = dsTemp.first()
println(device)

val dsTemp2 = ds
 .select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")
 .where("temp > 25")
 .as[DeviceTempByCountry]

// COMMAND ----------

// Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por defecto.

val mnm_file = "/databricks-datasets/learning-spark-v2/mnm_dataset.csv"

val mnm_df = (spark
              .read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnm_file))

println(mnm_df.printSchema)
println(mnm_df.schema)

// COMMAND ----------

// Cuando se define un schema al definir un campo por ejemplo
// StructField('Delay', FloatType(), True) ¿qué significa el último
// parámetro Boolean?

val schema_mnm_df = StructType(Array(
  StructField("State", StringType, true),
  StructField("Color", StringType, true),
  StructField("Count", IntegerType, true)))

// Indica que puede aceptar NULL si es true y no lo acepta si es false.

// COMMAND ----------

// Dataset vs DataFrame (Scala). ¿En qué se diferencian a nivel de código?


// COMMAND ----------

// Utilizando el mismo ejemplo utilizado en el capítulo para guardar en parquet y guardar
// los datos en los formatos:
// i. JSON
// ii. CSV (dándole otro nombre para evitar sobrescribir el fichero origen)
// iii. AVRO

fireTsDF.write.format("json").mode("overwrite").save("/tmp/fireServiceParquet/")
fireTsDF.write.format("csv").mode("overwrite").save("/tmp/fireServiceParquet/")
fireTsDF.write.format("avro").mode("overwrite").save("/tmp/fireServiceParquet/")
