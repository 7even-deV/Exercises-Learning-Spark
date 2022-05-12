# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# Ubicación y tipo de archivo
file_location = "/FileStore/tables/mnm_dataset.csv"
file_type = "csv"

# opciones CSV
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# Las opciones aplicadas son para archivos CSV. Para otros tipos de archivos, estos serán ignorados.
df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)

display(df)

# COMMAND ----------

# Crear una vista o tabla

temp_table_name = "mnm_dataset_csv"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Consulta la tabla temporal creada en una celda SQL */
# MAGIC 
# MAGIC select * from `mnm_dataset_csv`

# COMMAND ----------

# Con esto registrado como una vista temporal, solo estará disponible para este portátil en particular. Si desea que otros usuarios puedan consultar esta tabla, también puede crear una tabla desde DataFrame.
# Una vez guardada, esta tabla persistirá en los reinicios del clúster y permitirá que varios usuarios en diferentes portátiles consulten estos datos.
# Para hacerlo, elija el nombre de su tabla y descomente la línea inferior.

# permanent_table_name = "mnm_dataset_csv"

# df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# Importar las bibliotecas necesarias.
# Ya que estamos usando Python, importe SparkSession y funciones relacionadas
# del módulo PySpark.
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
#     if len(sys.argv) != 2:
#         print("Usage: mnmcount <file>", file=sys.stderr)
#         sys.exit(-1)

    # Cree una SparkSession utilizando las API de SparkSession.
    # Si no existe, cree una instancia. Allá
    # solo puede ser una SparkSession por JVM.
    spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())
        
    # Obtener el nombre de archivo del conjunto de datos de M&M a partir de los argumentos de la línea de comandos
    mnm_file = sys.argv[1]

    # Lea el archivo en un Spark DataFrame usando el CSV
    # formato infiriendo el esquema y especificando que el
    # El archivo contiene un encabezado, que proporciona nombres de columna para comas.
    # campos separados.
    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
#         .load(df))
        .csv(file_location))

    # Usamos las API de alto nivel de DataFrame. Nota
    # que no usamos RDD en absoluto. Porque algunos de Spark
    # funciones devuelven el mismo objeto, podemos encadenar llamadas a funciones.
    # 1. Seleccione del DataFrame los campos "Estado", "Color" y "Recuento"
    # 2. Ya que queremos agrupar cada estado y su recuento de colores M&M,
    # usamos groupBy()
    # 3. Recuentos agregados de todos los colores y groupBy() State and Color
    # 4 orderBy() en orden descendente
    count_mnm_df = (mnm_df
        .select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy("Total", ascending=False))

    # Muestra las agregaciones resultantes para todos los estados y colores;
    # un recuento total de cada color por estado.
    # Note show() es una acción, que desencadenará lo anterior
    # consulta a ejecutar.
    count_mnm_df.show(n=60, truncate=False)
    print("Total Rows = %d" % (count_mnm_df.count()))

# COMMAND ----------

# Si bien el código anterior se agregó y contó para todos
# los estados, ¿qué pasa si solo queremos ver los datos de
# un solo estado, por ejemplo, CA?
# 1. Seleccione de todas las filas en el DataFrame
# 2. Filtra solo el estado de CA
# 3. groupBy() Estado y Color como lo hicimos arriba
# 4. Agrega los conteos para cada color
# 5. orderBy() en orden descendente
# Encuentre el conteo agregado para California filtrando
ca_count_mnm_df = (mnm_df
    .select("State", "Color", "Count")
    .where(mnm_df.State == "CA")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total", ascending=False))

# Muestre la agregación resultante para California.
# Como arriba, show() es una acción que disparará la ejecución del
# cálculo completo.
ca_count_mnm_df.show(n=10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC i. Otras operaciones de agregación como el Max con otro tipo de ordenamiento
# MAGIC (descendiente).

# COMMAND ----------

from pyspark.sql import functions as f
ca_count_mnm_df.select(f.max("Total")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ii. Hacer un ejercicio como el “where” de CA que aparece en el libro pero
# MAGIC indicando más opciones de estados (p.e. NV, TX, CA, CO).

# COMMAND ----------

where_mnm_df = (mnm_df
    .select("State", "Color", "Count")
    .where((mnm_df.State == "NV") | (mnm_df.State == "TX")
         | (mnm_df.State == "CA") | (mnm_df.State == "CO"))
    .groupBy("State", "Color")
    .agg(count("Count")
    .alias("Total"))
    .orderBy("Total"))

where_mnm_df.show(n=10)

# COMMAND ----------

# MAGIC %md
# MAGIC iii. Hacer un ejercicio donde se calculen en una misma operación el Max, Min,
# MAGIC Avg, Count. Revisar el API (documentación) donde encontrarán este ejemplo:
# MAGIC ds.agg(max($"age"), avg($"salary"))
# MAGIC ds.groupBy().agg(max($"age"), avg($"salary"))
# MAGIC NOTA: $ es un alias de col()

# COMMAND ----------

from pyspark.sql import functions as col
where_mnm_df.select(col.max("Total"), col.min("Total"), col.avg("Total"), col.count("Total")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC iv. Hacer también ejercicios en SQL creando tmpView

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(Count) as `Max` from `mnm_dataset_csv` where `Color` = "Yellow"

# COMMAND ----------

count_mnm_df = (mnm_df
 .select("State", "Color", "Count")
 .groupBy("State", "Color")
 .agg(count("Count")
 .alias("Total"))
 .orderBy("Total", ascending=False))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT State, Color, Count, sum(Count) AS Total
# MAGIC -- FROM mnm_df
# MAGIC -- GROUP BY State, Color, Count
# MAGIC -- ORDER BY Total DESC
