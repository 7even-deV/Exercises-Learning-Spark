# Databricks notebook source
from pyspark.sql.types import LongType

# Create cubed function
def cubed(s):
    return s * s * s

# Register UDF
spark.udf.register("cubed", cubed, LongType())

# Generate temporary view
spark.range(1, 9).createOrReplaceTempView("udf_test")

# COMMAND ----------

# Query the cubed UDF
spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

# COMMAND ----------

# Import pandas
import pandas as pd

# Import various pyspark SQL functions including pandas_udf
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

# Declare the cubed function
def cubed(a: pd.Series) -> pd.Series:
    return a * a * a

# Create the pandas UDF for the cubed function
cubed_udf = pandas_udf(cubed, returnType=LongType())

# COMMAND ----------

# Create a Pandas Series
x = pd.Series([1, 2, 3])

# The function for a pandas_udf executed with local Pandas data
print(cubed(x))

# COMMAND ----------

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.range(1, 4)

# Execute function as a Spark vectorized UDF
df.select("id", cubed_udf(col("id"))).show()

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([StructField("celsius", ArrayType(IntegerType()))])
t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")

# Show the DataFrame
t_c.show()

# COMMAND ----------

# transform()
# Calculate Fahrenheit from Celsius for an array of temperatures
spark.sql("""
SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC
""").show()

# COMMAND ----------

# filter()
# Filter temperatures > 38C for array of temperatures
spark.sql("""
SELECT celsius,
 filter(celsius, t -> t > 38) as high FROM tC
""").show()

# COMMAND ----------

# exists()
# Is there a temperature of 38C in the array of temperatures
spark.sql("""
SELECT celsius,
 exists(celsius, t -> t = 38) as threshold FROM tC
""").show()

# COMMAND ----------

# reduce()
# Calculate average temperature and convert to F
spark.sql("""
SELECT celsius,
 reduce(celsius, 0, (t, acc) -> t + acc, acc -> (acc div size(celsius) * 9 div 5) + 32) as avgFahrenheit FROM tC
""").show()

# COMMAND ----------

# Set file paths
from pyspark.sql.functions import expr

tripdelaysFilePath = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
airportsnaFilePath = "/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

# Obtain airports data set
airportsna = (spark.read
    .format("csv")
    .options(header="true", inferSchema="true", sep="\t")
    .load(airportsnaFilePath))
airportsna.createOrReplaceTempView("airports_na")

# Obtain departure delays data set
departureDelays = (spark.read
    .format("csv")
    .options(header="true")
    .load(tripdelaysFilePath))
departureDelays = (departureDelays
    .withColumn("delay", expr("CAST(delay as INT) as delay"))
    .withColumn("distance", expr("CAST(distance as INT) as distance")))
departureDelays.createOrReplaceTempView("departureDelays")

# Create temporary small table
foo = (departureDelays
    .filter(expr("""origin == 'SEA' and destination == 'SFO' and date like '01010%' and delay > 0""")))
foo.createOrReplaceTempView("foo")

# COMMAND ----------

spark.sql("SELECT * FROM airports_na LIMIT 10").show()

# COMMAND ----------

# Union two tables
bar = departureDelays.union(foo)
bar.createOrReplaceTempView("bar")

# Show the union (filtering for SEA and SFO in a specific time range)
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
AND date LIKE '01010%' AND delay > 0""")).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS departureDelaysWindow;
# MAGIC 
# MAGIC CREATE TABLE departureDelaysWindow AS
# MAGIC SELECT origin, destination, SUM(delay) AS TotalDelays
# MAGIC  FROM departureDelays
# MAGIC WHERE origin IN ('SEA', 'SFO', 'JFK')
# MAGIC  AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
# MAGIC GROUP BY origin, destination;
# MAGIC 
# MAGIC SELECT * FROM departureDelaysWindow

# COMMAND ----------

spark.sql("""
SELECT origin, destination, TotalDelays, rank
  FROM (
  SELECT origin, destination, TotalDelays, dense_rank()
  OVER (PARTITION BY origin ORDER BY TotalDelays DESC) as rank
  FROM departureDelaysWindow) t
  WHERE rank <= 3
  """).show()

# COMMAND ----------

foo.show()

# COMMAND ----------



# COMMAND ----------

# Modifications
foo.show()

# COMMAND ----------

# Adding new columns
from pyspark.sql.functions import expr

foo2 = (foo.withColumn("status", expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")))

# COMMAND ----------

foo2.show()

# COMMAND ----------

# Dropping columns
foo3 = foo2.drop("delay")
foo3.show()

# COMMAND ----------

# Renaming columns
foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pivoting
# MAGIC SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
# MAGIC FROM departureDelays
# MAGIC WHERE origin = 'SEA'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
# MAGIC  FROM departureDelays WHERE origin = 'SEA'
# MAGIC )
# MAGIC 
# MAGIC PIVOT (
# MAGIC  CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
# MAGIC  FOR month IN (1 JAN, 2 FEB)
# MAGIC )
# MAGIC 
# MAGIC ORDER BY destination

# COMMAND ----------

# MAGIC %md
# MAGIC Pros y Cons utilizar UDFs

# COMMAND ----------

# Pros: Su flexibilidad de uso.

# Cons: No persiste por debajo del MetaStore y, por tanto, solo sirve para una sesión. Es más laborioso que utilizar las funciones ya definidas por Spark.
