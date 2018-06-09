package simpletiler

import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.io.kryo.KryoRegistrator
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer

import scala.io.StdIn

object Main {

  def createAllSparkConf(): SparkConf = {
    /**
      * # -- MEMORY ALLOCATION -- #
        spark.master                   yarn
        spark.driver.memory            512m
        spark.yarn.am.memory           512m
        spark.executor.memory          512m


        # -- MONITORING -- #
        spark.eventLog.enabled            true
        spark.eventLog.dir                hdfs://sh-master:9000/spark-logs
        spark.history.provider            org.apache.spark.deploy.history.FsHistoryProvider
        spark.history.fs.logDirectory     hdfs://sh-master:9000/spark-logs
        spark.history.fs.update.interval  3s
        spark.history.ui.port             18080
        spark.ui.enabled                  true

      */
    new SparkConf()
      //      .setMaster("yarn")
//      .setMaster("local")
      .setAppName("Thesis")
      .set("spark.serializer",        classOf[KryoSerializer].getName)
      .set("spark.kryo.registrator",  classOf[KryoRegistrator].getName)
      .set("spark.driver.memory",     "1024m")
      .set("spark.yarn.am.memory",    "1024m")
      .set("spark.executor.memory",   "1024m")
      .set("spark.eventLog.enabled",            "true")
      .set("spark.eventLog.dir",                "/home/spark/spark/logs")
      //      .set("spark.eventLog.dir",                "hdfs://sh-master:9000/spark-logs")
      .set("spark.history.provider",            "org.apache.spark.deploy.history.FsHistoryProvider")
      //      .set("spark.history.fs.logDirectory",     "hdfs://sh-master:9000/spark-logs")
      .set("spark.history.fs.logDirectory",     "/home/spark/spark/logs")
      .set("spark.history.fs.update.interval",  "3s")
      .set("spark.history.ui.port",             "18080")
      .set("spark.ui.enabled",                  "true")

    //.set("spark.default.parallelism", "2")
    //.set("spark.akka.frameSize", "512")
    //.set("spark.kryoserializer.buffer.max.mb", "800")
  }

  def main(args: Array[String]): Unit = {
    //Initialize
    println("\n\n>>> INITIALIZING <<<\n\n")

    val sc = new SparkContext(createAllSparkConf())
    val run_reps = args(1).toInt

    try {

      println("ARGUMENTS:")
      pprint.pprintln(args)

      args(0) match {
        case "simple" => run_simple_read_tile_query(
          //simpleReadTileQuery(run_rep,src_raster_file_path, tile_out_path, meta_shp_path, qry_shp_path, output_gtif_path)
          run_reps, args(2),args(3),args(4),args(5),args(6))(sc)
        case _ => println("ERROR: Invalid first CLI argument")
      }
          // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040
      println("Hit enter to exit.")
      StdIn.readLine()
    } finally {
      sc.stop()
    }
  }

  def run_test()(implicit sc: SparkContext) = {
    println("Hello there!")
  }

  def run_simple_read_tile_query(run_reps: Int, src_raster_file_path: String, tile_out_path: String, meta_shp_path : String, qry_shp_path : String, output_gtif_path : String )
                                (implicit sc: SparkContext): Unit = {
    for( run_rep <- 1 to run_reps) {
      simpleReadTileQuery(run_rep,src_raster_file_path, tile_out_path, meta_shp_path, qry_shp_path, output_gtif_path)
    }
  }
}
