package scala

import com.mongodb.spark._
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.{SparkConf, SparkContext}

object MongoSparkTest extends App with LazyLogging {

  val conf = new SparkConf()
    .setAppName("mongoexample")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)

  // Read a collection from MongoDB
  val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "test", "collection" -> "zips"))

  // Save the collection into a DataFrame
  val testDF = sc.loadFromMongoDB(readConfig).toDF()

  // Select some columns from the DF and create a new one
  val mod = testDF.select("city", "pop", "state")

  // Set the config to write the new DF in the database
  val writeConfig = WriteConfig(
    Map("uri" -> "mongodb://127.0.0.1/spark.processing", "database" -> "test", "collection" -> "test"))

  // Write the new DF in a new collection
  MongoSpark.save(mod.write.option("collection", "collNameToUpdate").mode("overwrite"), writeConfig)

  // Read the new collection to check if it was saved as expected
  val newReadConfig = ReadConfig(
    Map("uri" -> "mongodb://127.0.0.1/spark.processing", "database" -> "test", "collection" -> "test"))
  
  val rModifiedDF = sc.loadFromMongoDB(newReadConfig).toDF()

  // Show the content
  rModifiedDF.show()

}
