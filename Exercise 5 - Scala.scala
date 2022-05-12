// Databricks notebook source
// Create cubed function
val cubed = (s: Long) => {
 s * s * s
}

// Register UDF
spark.udf.register("cubed", cubed)

// Create temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

// COMMAND ----------

// Query the cubed UDF
spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

// COMMAND ----------

// Create DataFrame with two rows of two arrays (tempc1, tempc2)
val t1 = Array(35, 36, 32, 30, 40, 42, 38)
val t2 = Array(31, 32, 34, 55, 56)
val tC = Seq(t1, t2).toDF("celsius")
tC.createOrReplaceTempView("tC")

// Show the DataFrame
tC.show()

// COMMAND ----------

// transform()
// Calculate Fahrenheit from Celsius for an array of temperatures
spark.sql("""
SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC
""").show()

// COMMAND ----------

// filter()
// Filter temperatures > 38C for array of temperatures
spark.sql("""
SELECT celsius,
 filter(celsius, t -> t > 38) as high FROM tC
""").show()

// COMMAND ----------

// exists()
// Is there a temperature of 38C in the array of temperatures
spark.sql("""
SELECT celsius,
 exists(celsius, t -> t = 38) as threshold FROM tC
""").show()

// COMMAND ----------

// reduce()
// Calculate average temperature and convert to F
spark.sql("""
SELECT celsius,
 reduce(celsius, 0, (t, acc) -> t + acc, acc -> (acc div size(celsius) * 9 div 5) + 32) as avgFahrenheit FROM tC
""").show()

// COMMAND ----------

import org.apache.spark.sql.functions._

// Set file paths
val delaysPath =
 "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
val airportsPath =
 "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

// Obtain airports data set
val airports = spark.read
 .option("header", "true")
 .option("inferschema", "true")
 .option("delimiter", "\t")
 .csv(airportsPath)
airports.createOrReplaceTempView("airports_na")

// Obtain departure Delays data set
val delays = spark.read
 .option("header","true")
 .csv(delaysPath)
 .withColumn("delay", expr("CAST(delay as INT) as delay"))
 .withColumn("distance", expr("CAST(distance as INT) as distance"))
delays.createOrReplaceTempView("departureDelays")

// Create temporary small table
val foo = delays.filter(
 expr("""origin == 'SEA' AND destination == 'SFO' AND
 date like '01010%' AND delay > 0"""))
foo.createOrReplaceTempView("foo")

// COMMAND ----------

spark.sql("SELECT * FROM airports_na LIMIT 10").show()

// COMMAND ----------

// Union two tables
val bar = delays.union(foo)
bar.createOrReplaceTempView("bar")
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
AND date LIKE '01010%' AND delay > 0""")).show()

// COMMAND ----------

foo.join(
 airports.as('air),
 $"air.IATA" === $"origin"
).select("City", "State", "date", "delay", "distance", "destination").show()

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS departureDelaysWindow;
// MAGIC 
// MAGIC CREATE TABLE departureDelaysWindow AS
// MAGIC SELECT origin, destination, SUM(delay) AS TotalDelays
// MAGIC  FROM departureDelays
// MAGIC WHERE origin IN ('SEA', 'SFO', 'JFK')
// MAGIC  AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
// MAGIC GROUP BY origin, destination;
// MAGIC 
// MAGIC SELECT * FROM departureDelaysWindow

// COMMAND ----------

spark.sql("""
SELECT origin, destination, TotalDelays, rank
  FROM (
  SELECT origin, destination, TotalDelays, dense_rank()
  OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
  FROM departureDelaysWindow) t
  WHERE rank <= 3
  """).show()

// COMMAND ----------

Modifications
foo.show()

// COMMAND ----------

import org.apache.spark.sql.functions.expr

val foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
 )


// COMMAND ----------

// Adding new columns
import org.apache.spark.sql.functions.expr

val foo2 = foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END"))

// COMMAND ----------

foo2.show()

// COMMAND ----------

// Dropping columns
val foo3 = foo2.drop("delay")
foo3.show()

// COMMAND ----------

// Renaming columns
val foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Pivoting
// MAGIC SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
// MAGIC FROM departureDelays
// MAGIC WHERE origin = 'SEA'

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM (
// MAGIC SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
// MAGIC  FROM departureDelays WHERE origin = 'SEA'
// MAGIC )
// MAGIC 
// MAGIC PIVOT (
// MAGIC  CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
// MAGIC  FOR month IN (1 JAN, 2 FEB)
// MAGIC )
// MAGIC 
// MAGIC ORDER BY destination

// COMMAND ----------

// MAGIC %md
// MAGIC Pros y Cons utilizar UDFs

// COMMAND ----------

// Pros: Su flexibilidad de uso.

// Cons: No persiste por debajo del MetaStore y, por tanto, solo sirve para una sesión. Es más laborioso que utilizar las funciones ya definidas por Spark.
