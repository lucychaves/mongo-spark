package com

import com.mongodb.spark._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}

object MongoSparkTest extends App with LazyLogging {

  val conf = new SparkConf()
    .setAppName("mongoexample")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

  // Lee una collection de la base y la guarda en un DF
  val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "zips"))
  val testDF = sc.loadFromMongoDB(readConfig).toDF()

  // Selecciona unas columnas del DF leido y crea uno nuevo
  val mod = testDF.select("city", "pop", "state")

  // Settea la config para escribir el nuevo DF en la base
  val writeConfig = WriteConfig(
    Map("uri" -> "mongodb://127.0.0.1/spark.processing", "database" -> "test", "collection" -> "test"))

  // Escribe el DF en una nueva collection
  MongoSpark.save(mod.write.option("collection", "collNameToUpdate").mode("overwrite"), writeConfig)

  // Lee la nueva collection y la guarda en un DF
  val newReadConfig = ReadConfig(
    Map("uri" -> "mongodb://127.0.0.1/spark.processing", "database" -> "test", "collection" -> "test"))
  val rModifiedDF = sc.loadFromMongoDB(newReadConfig).toDF()

  // Muestra el contenido
  rModifiedDF.show()

}
