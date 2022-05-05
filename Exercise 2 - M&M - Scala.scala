// Databricks notebook source
// Ubicación y carga de archivo
val mnm_file = spark.read.text("/FileStore/tables/mnm_dataset.csv")

display(mnm_file)

// COMMAND ----------

val mnm_df = (spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(mnm_file))

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC /* Consulta la tabla temporal creada en una celda SQL */
// MAGIC 
// MAGIC select * from `mnm_df`

// COMMAND ----------

// Importar las bibliotecas necesarias.
// Ya que estamos usando Scala, importe SparkSession y funciones relacionadas
// del módulo Scala.

package main.scala.chapter2
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Usage: MnMcount <mnm_file_dataset>
 */

object MnMcount {
def main(args: Array[String]) {
    val spark = SparkSession
    .builder
    .appName("MnMCount")
    .getOrCreate()
//     if (args.length < 1){
//         print("Usage: MnMcount <mnm_file_dataset>")
//         sys.exit(1)}

    // Obtener el nombre de archivo del conjunto de datos de M&M
    val mnmFile = args(0)

    // Lee el archivo en un Spark DataFrame
    val mnmDF = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
//         .load(mnmFile)
        .csv(mnm_df))

    // Recuentos agregados de todos los colores y groupBy() State and Color
     // orderBy() en orden descendente
    val countMnMDF = mnmDF
        .select("State", "Color", "Count")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy(desc("Total"))

    // Mostrar las agregaciones resultantes para todos los estados y colores
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    // Encuentre los conteos agregados para California filtrando
    val caCountMnMDF = mnmDF
        .select("State", "Color", "Count")
        .where(col("State") === "CA")
        .groupBy("State", "Color")
        .agg(count("Count").alias("Total"))
        .orderBy(desc("Total"))

    // Mostrar las agregaciones resultantes para California
    caCountMnMDF.show(10)

    // Detener la SparkSession
    spark.stop()
    }
}


// COMMAND ----------

// Name of the package
name := "main/scala/chapter2"
// Version of our package
version := "1.0"
// Version of Scala
scalaVersion := "2.12.10"
// Spark library dependencies
libraryDependencies ++= Seq(
 "org.apache.spark" %% "spark-core" % "3.0.0-preview2",
 "org.apache.spark" %% "spark-sql" % "3.0.0-preview2"
)
