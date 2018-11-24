package simpletiler

import java.io.{BufferedInputStream, File}
import java.util.UUID
import java.util.zip.GZIPInputStream

import derive.key
import geotrellis.raster._
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.spark._
import geotrellis.spark.io.kryo.KryoRegistrator
import geotrellis.spark.tiling.{LayoutDefinition, MapKeyTransform}
import geotrellis.vector.{Extent, Feature, Geometry, MultiPolygon, MultiPolygonFeature, Polygon}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.SizeEstimator

import scala.io.StdIn
import simpletiler.SimpleTilingFramework._
import simpletiler.SFCIndexing._
import simpletiler.Constants._

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
      .set("spark.yarn.am.memory",    "2048m")
      .set("spark.driver.memory",     "2048m")
      .set("spark.executor.memory",   "2048m")
      .set("spark.eventLog.enabled",            "true")
      //.set("spark.eventLog.dir",                "/home/spark/spark/logs")
//      .set("spark.eventLog.dir",                "/home/ken/spark/logs")
      .set("spark.eventLog.dir",                "hdfs://sh-master:9000/spark-logs")
      .set("spark.history.provider",            "org.apache.spark.deploy.history.FsHistoryProvider")
      .set("spark.history.fs.logDirectory",     "hdfs://sh-master:9000/spark-logs")
      //.set("spark.history.fs.logDirectory",     "/home/spark/spark/logs")
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

    implicit val sc = new SparkContext(createAllSparkConf())
    implicit val hdfs = fs.FileSystem.get(sc.hadoopConfiguration)

    val run_reps = args(1).toInt

    try {

      println("ARGUMENTS:")
      pprint.pprintln(args)

      // Check and create temp directories
      val tmp_dir : File = new File(Constants.TMP_DIRECTORY)
      if (!tmp_dir.exists) tmp_dir.mkdir
      val tmp_shp_dir : File = new File(Constants.TMP_SHP_DIRECTORY)
      if (!tmp_shp_dir.exists) tmp_shp_dir.mkdir


      args(0) match {
        case "simple" => run_simple_read_tile_query(
          run_reps, args(2),args(3),args(4),args(5),args(6))
        case "download" => run_download(
          run_reps, args(2))
        case "files" => getListOfFilesFromHDFS(args(2))

        case "jointest" => testClipToGrid_2(args(2),args(3),args(4))
        //def testClipToGrid(tile_dir_path: String, metadata_shp_filepath: String, sfc_index_label: String)

        case _ => println("ERROR: Invalid first CLI argument")
      }
      // Pause to wait to close the spark context,
      // so that you can check out the UI at http://localhost:4040

      // Delete temp directory if it exists
      //if (tmp_dir.exists) tmp_dir.delete()

      println("Hit enter to exit.")
      //StdIn.readLine()
    } finally {
      sc.stop()
    }
  }

  def run_test()(implicit sc: SparkContext) = {
    println("Hello there!")
  }

  def run_download(run_reps: Int, hdfs_shp_path: String)
                  (implicit sc: SparkContext, hdfs: fs.FileSystem) = {
    downloadSHPtoTempShpDir(hdfs_shp_path)
  }

  def run_simple_read_tile_query(run_reps: Int, src_raster_file_path: String, tile_out_path: String, meta_shp_path : String, qry_shp_path : String, output_gtif_path : String )
                                (implicit sc: SparkContext, hdfs: fs.FileSystem): Unit = {
    for( run_rep <- 1 to run_reps) {
      simpleReadTileQuery(run_rep,src_raster_file_path, tile_out_path, meta_shp_path, qry_shp_path, output_gtif_path)
    }
  }

  //TODO: Test Functions

  def getListOfFilesFromHDFS(dir_path: String)
                            (implicit sc: SparkContext, hdfs: fs.FileSystem): Unit = {
    val status = hdfs.listStatus(new fs.Path(dir_path))
    println("HDFS LIST:")
    status.foreach(x => println(x.getPath))

    println("SC BINARY FILES LIST:")
    //    sc.binaryFiles(dir_path).collect.foreach(println(_))
    val result_rdd: RDD[(String, MultibandGeoTiff)]= sc.binaryFiles(dir_path).filter{ tuple =>
      tuple._1.endsWith(".tif")
    }.map{ case (hdfs_filepath: String, pds: PortableDataStream) =>
    {
      val stream: BufferedInputStream = new BufferedInputStream(pds.open(), 2048)
      val mband_gtiff = GeoTiffReader.readMultiband(IOUtils.toByteArray(stream))

      (hdfs_filepath.split("/").last, mband_gtiff)
    }}

    // Test Results
    result_rdd.collect.foreach{ case (hdfs_filepath: String, mband_gtif: MultibandGeoTiff) =>
      println(s"${hdfs_filepath} : ${mband_gtif.extent.toString()}")
    }
    result_rdd
  }

  def testClipToGrid(tile_dir_path: String, metadata_shp_filepath: String, sfc_index_label: String)
                    (implicit sc: SparkContext, hdfs: fs.FileSystem): RDD[(SpatialKey, Iterable[Feature[MultiPolygon, Map[String, Object]]])] = {
    val metadata_fts : Seq[MultiPolygonFeature[Map[String,Object]]] = ShapeFileReader.readMultiPolygonFeatures(metadata_shp_filepath)
    val metadata_fts_rdd : RDD[MultiPolygonFeature[Map[String,Object]]]= sc.parallelize(metadata_fts, Constants.RDD_PARTS)

    val gtif_tiles_rdd = getGeotiffTilesRDD(tile_dir_path)

    // Getting merged tile extents
    val raster_merged_extents = getMergedTileExtents(gtif_tiles_rdd)

    val layout = LayoutDefinition(
      GridExtent(
        raster_merged_extents,
        Constants.GRID_CELL_WIDTH,
        Constants.GRID_CELL_HEIGHT
      ),
      Constants.TILE_SIZE,
      Constants.TILE_SIZE
    )

//    val groupedPolys: RDD[(SpatialKey, Iterable[MultiPolygonFeature[Map[String,Object]]])] =
//      metadata_fts_rdd.clipToGrid(layout)
//        .flatMap { case (key, feature) =>
//          val mpFeature: Option[MultiPolygonFeature[Map[String,Object]]]] =
//            feature.geom match {
//              case p: Polygon => Some(feature.mapGeom(_ => MultiPolygon(p)))
//              case mp: MultiPolygon => Some(feature.mapGeom(_ => mp))
//              case _ => None
//            }
//          mpFeature.map { mp => (key, mp) }
//        }
//        .groupByKey(new HashPartitioner(1000))
    val data_key = "UID"
    val cliptogrid_features: RDD[(SpatialKey, Feature[Geometry, Map[String, Object]])] =
      metadata_fts_rdd.clipToGrid(layout)

    cliptogrid_features.collect.foreach{
      case (key, feature) =>
        println(s"[$key] => ${feature.data(data_key)}")
    }
    val grouped_polys = cliptogrid_features.flatMap { case (key, feature) =>
                val mpFeature =
                  feature.geom match {
                    case p: Polygon => Some(feature.mapGeom(_ => MultiPolygon(p)))
                    case mp: MultiPolygon => Some(feature.mapGeom(_ => mp))
                    case _ => None
                  }
                mpFeature.map { mp => (key, mp) }
              }.groupByKey(new HashPartitioner(1000))
      .filter(x => (x._1._1 >= 0) && (x._1._2 >= 0) )

    println("len_count - gtif_tiles_rdd:   "+gtif_tiles_rdd.count())
    println("len_count - metadata_fts_rdd: "+metadata_fts_rdd.count())
    println("len_count - cliptogrid_feats: "+cliptogrid_features.count())
    println("len_count - grouped_polys:    "+grouped_polys.count())
    grouped_polys
  }


  def testClipToGrid_2(tile_dir_path: String, metadata_shp_filepath: String, sfc_index_label: String)
                    (implicit sc: SparkContext, hdfs: fs.FileSystem): Unit = {
    val metadata_fts : Seq[MultiPolygonFeature[Map[String,Object]]] = ShapeFileReader.readMultiPolygonFeatures(metadata_shp_filepath)
    val metadata_fts_rdd : RDD[MultiPolygonFeature[Map[String,Object]]]= sc.parallelize(metadata_fts, Constants.RDD_PARTS)

    val gtif_tiles_rdd = getGeotiffTilesRDD(tile_dir_path)

    val raster_merged_extents = getMergedTileExtents(gtif_tiles_rdd)

    val tile_layout = LayoutDefinition(
      GridExtent(
        raster_merged_extents,
        Constants.GRID_CELL_WIDTH,
        Constants.GRID_CELL_HEIGHT
      ),
      Constants.TILE_SIZE,
      Constants.TILE_SIZE
    )

    val map_transform =tile_layout.mapTransform

    val keys_per_ft = createSpatialKeysToFeaturesMapping(map_transform, metadata_fts_rdd)

    keys_per_ft.collect.foreach{
      case (spatial_key, ft_list) =>
        print(s"SK(${spatial_key._1},${spatial_key._2}) : [")
        ft_list.foreach { feature =>
          print(s"${feature.data("UID")}, ")
        }
        println("]")
    }
  }

  def createSpatialKeysToFeaturesMapping(map_transform: MapKeyTransform, metadata_fts_rdd : RDD[MultiPolygonFeature[Map[String,Object]]]): RDD[(SpatialKey, Iterable[MultiPolygonFeature[Map[String, Object]]])] ={
    // Compute Spatial Keys per Geometry Feature
    return metadata_fts_rdd.flatMap { feature =>
      val keys = map_transform.keysForGeometry(feature.geom)
      keys.map{ key => (key, feature) }
    }.filter(x => (x._1._1 >= 0) && (x._1._2 >= 0) ).groupByKey() // Filter negative spatial keys (out of bounds)
  }


  def getMergedTileExtents(gtif_tiles_rdd: RDD[(String, MultibandGeoTiff)]): Extent ={
    // Getting merged tile extents
    val sample_gtif = gtif_tiles_rdd.take(1)(0)._2
    val tile_crs: geotrellis.proj4.CRS = sample_gtif.crs
    val cell_type: CellType = sample_gtif.cellType

    val x_vals = scala.collection.mutable.ArrayBuffer.empty[Double]
    val y_vals = scala.collection.mutable.ArrayBuffer.empty[Double]

    println("sizeEstimate - gtif_tiles_rdd: "+SizeEstimator.estimate(gtif_tiles_rdd).toString)


    gtif_tiles_rdd.collect.foreach{
      case (filename, mband_gtif) =>
        x_vals += mband_gtif.extent.xmin
        x_vals += mband_gtif.extent.xmax
        y_vals += mband_gtif.extent.ymin
        y_vals += mband_gtif.extent.ymax
    }

    println("len_count - x_vals: "+x_vals.length)
    println("len_count - y_vals: "+y_vals.length)


    new Extent(x_vals.min, y_vals.min, x_vals.max, y_vals.max)
  }

  def getGeotiffTilesRDD(tile_dir_path: String)
                        (implicit sc: SparkContext, hdfs: fs.FileSystem): RDD[(String, MultibandGeoTiff)] ={
    if (tile_dir_path contains "hdfs:") {
      sc.binaryFiles(tile_dir_path).filter{ tuple =>
        tuple._1.endsWith(".tif")
      }.map{ case (hdfs_filepath: String, pds: PortableDataStream) =>
      {
        val stream: BufferedInputStream = new BufferedInputStream(pds.open(), 2048)
        val mband_gtiff = GeoTiffReader.readMultiband(IOUtils.toByteArray(stream))

        val filename = hdfs_filepath.split("/").last
        println(s"Read GeoTiff Tile - [$filename]")

        (filename, mband_gtiff)
      }}
    }else{
      val dir = new File(tile_dir_path)
      sc.parallelize(dir.listFiles.filter(_.isFile).toList.filter { file =>
        file.getName.endsWith(".tif")
      }.map { tif_file =>
        (tif_file.getName, GeoTiffReader.readMultiband(tif_file.getAbsolutePath))
      })
    }
  }

}
