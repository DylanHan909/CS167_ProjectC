package edu.ucr.cs.cs167.groupC2

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {
        case "task1" =>
          val wildfireDF = sparkSession.read.format("csv")
            .option("sep", "\t")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(inputFile)
          val wildfireWithGeometryRDD: SpatialRDD = wildfireDF.selectExpr("x", "y", "acq_date", "double(split(frp,',')[0]) AS frp", "acq_time", "ST_CreatePoint(x,y) AS geometry").toSpatialRDD
          val countiesDF = sparkSession.read.format("shapefile").load("tl_2018_us_county.zip")
          val countiesRDD: SpatialRDD = countiesDF.toSpatialRDD
          val wildFireSpatialJoinRDD: RDD[(IFeature, IFeature)] = wildfireWithGeometryRDD.spatialJoin(countiesRDD)
          val wildfireCountyAdd: DataFrame = wildFireSpatialJoinRDD.map({ case (wildfire, county) => Feature.append(wildfire, county.getAs[String]("GEOID"), "County") })
            .toDataFrame(sparkSession)
//          wildfireCountyAdd.printSchema()
          val convertedDF: DataFrame = wildfireCountyAdd.selectExpr("x", "y", "acq_date", "frp", "acq_time", "County").drop("geometry")
//          convertedDF.printSchema()
          convertedDF.write.mode(SaveMode.Overwrite).parquet("wildfiredb_ZIP")
        case "task2" =>
        case _ => validOperation = false
      }
      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")
    } finally {
      sparkSession.stop()
    }
  }
}
