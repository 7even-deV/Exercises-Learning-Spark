# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# MAGIC %python
# MAGIC dfpy = spark.read.text("/FileStore/tables/el_quijote-1.txt")
# MAGIC display(dfpy)

# COMMAND ----------

# MAGIC %scala
# MAGIC val dfsc = spark.read.text("/FileStore/tables/el_quijote-1.txt")
# MAGIC display(dfsc)

# COMMAND ----------

# MAGIC %md
# MAGIC # Aplicar no solo count (para obtener el número de líneas) y show sino probar distintas
# MAGIC # sobrecargas del método show (con/sin truncate, indicando/sin indicar num de filas,
# MAGIC # etc) así como también los métodos, head, take, first (diferencias entre estos 3?)

# COMMAND ----------

# MAGIC %python
# MAGIC dfpy.count()

# COMMAND ----------

# MAGIC %scala
# MAGIC dfsc.count()

# COMMAND ----------

# MAGIC %python
# MAGIC dfpy.show(10, truncate=True)

# COMMAND ----------

# MAGIC %scala
# MAGIC dfsc.show(10, truncate=true)

# COMMAND ----------

# MAGIC %python
# MAGIC dfpy.show(truncate=False)

# COMMAND ----------

# MAGIC %scala
# MAGIC dfsc.show(truncate=false)

# COMMAND ----------

# MAGIC %python
# MAGIC dfpy.head()
# MAGIC # Devuelve el primer row del texto (encabezado)

# COMMAND ----------

# MAGIC %python
# MAGIC dfpy.take(1)
# MAGIC # Devuelve una lista de rows (tantas como n indiques)

# COMMAND ----------

# MAGIC %python
# MAGIC dfpy.first()
# MAGIC # Aparentemente devuelve lo mismo que el head()

# COMMAND ----------

# MAGIC %scala
# MAGIC dfsc.head()
# MAGIC // Devuelve el primer row del texto (encabezado)

# COMMAND ----------

# MAGIC %scala
# MAGIC dfsc.take(1)
# MAGIC // Devuelve una lista de rows (tantas como n indiques)

# COMMAND ----------

# MAGIC %scala
# MAGIC dfsc.first()
# MAGIC // Aparentemente devuelve lo mismo que el head()
