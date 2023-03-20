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
        // args: task1 wildfiredb_1k.csv
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

        // args: task2 wildfiredb_ZIP 01/01/2016 12/31/2017
        case "task2" =>
          // Load the dataset in the Parquet format.
          sparkSession.read.parquet(inputFile)
            .createOrReplaceTempView("wildfire")

          val startDate: String = args(2)
          val endDate: String = args(3)
          // Run an SQL query that does the following
          sparkSession.sql(
            s"""
            SELECT County, SUM(frp) AS fire_intens
            FROM wildfire
            WHERE to_date(acq_date, 'yyyy-MM-dd') BETWEEN to_date('""" + startDate + """', 'MM/dd/yyyy') AND to_date('""" + endDate + """', 'MM/dd/yyyy')
            GROUP BY County;
            """).createOrReplaceTempView("wild_query")
            //.foreach(row => println(s"${row.get(0)}\t${row.get(1)}"))

          // Load the county dataset using Beast and convert to a Dataframe.
          sparkContext.shapefile("tl_2018_us_county.zip")
            .toDataFrame(sparkSession)
            .createOrReplaceTempView("counties")

          // counties.ALAND, counties.AWATER, counties.CBSAFP, counties.CLASSFP, counties.COUNTYFP, counties.COUNTYNS, counties.CSAFP, wild_query.County, counties.FUNCSTAT, counties.GEOID, counties.INTPTLAT, counties.INTPTLON, counties.LSAD, counties.METDIVFP, counties.MTFCC, counties.NAME, counties.NAMELSAD, counties.STATEFP, counties.g, wild_query.fire_intens
          // Run an equi-join SQL query to join with the results of the previous query on GEOID=County. Select the county name, the geometry, and the fire_intensity.
          sparkSession.sql(
            s"""
            SELECT GEOID, NAME, g, fire_intens
            FROM wild_query, counties
            WHERE GEOID=County
            """)//.foreach(row => println(s"${row.get(0)}\t${row.get(1)}\t${row.get(2)}"))

            // Convert the result back to an RDD and write as a Shapefile named wildfireIntensityCounty. You might want to use coalesce(1) to ensure that only one output file is written.
            .toSpatialRDD
            .coalesce(1)
            .saveAsShapefile("wildfireIntensityCounty")

        // args:
        case "task3" =>
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
