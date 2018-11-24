package simpletiler

import java.io.{BufferedOutputStream, File, PrintWriter}

import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.resample.Bilinear
import geotrellis.raster.{MultibandTile, _}
import geotrellis.spark._
import geotrellis.spark.io.Intersects
import geotrellis.spark.io.hadoop._
import geotrellis.spark.tiling._
import geotrellis.util._
import geotrellis.vector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.SizeEstimator
import simpletiler.Constants._
import simpletiler.UtilFunctions._
import org.apache.hadoop.fs
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

object SimpleTilingFramework {
  def simpleReadTileQuery(run_rep: Int, src_raster_file_path: String, tile_out_path: String, meta_shp_path : String, qry_shp_path : String, output_gtif_path : String )
                         (implicit sc: SparkContext, hdfs: fs.FileSystem): Unit = {
    println("SIMPLE RUN REP: " + run_rep.toString)

    val time_tiling = time{
      simpleReadGeotiffAndTile(src_raster_file_path, tile_out_path)
    }

    val (reprojected_rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]],
    raster_metadata) = time_tiling._1

    val time_data_query = time {
      simpleQueryTilesWithShp(reprojected_rdd, qry_shp_path, output_gtif_path)
    }

    val output_json_path = output_gtif_path.replaceAll("\\.[^.]*$", "") + ".json"

    val time_meta_query = time {
      simpleQueryMetadataWithShp(reprojected_rdd, qry_shp_path, output_json_path)
    }

    println("METRIC: time_tiling     -- " + time_tiling._2)
    println("METRIC: time_data_query -- " + time_data_query._2)
    println("METRIC: time_meta_query -- " + time_meta_query._2)

    //hdfs.close()

  }

  def simpleReadGeotiffAndTile(file_path: String, tile_out_path: String)
                              (implicit sc: SparkContext): (RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], TileLayerMetadata[SpatialKey])= {
    val extent_tile_rdd: RDD[(ProjectedExtent, MultibandTile)] =
      sc.hadoopMultibandGeoTiffRDD(file_path)

    // Tiling layout to TILE_SIZE x TILE_SIZE grids
    val (_, rasterMetaData) =
      TileLayerMetadata.fromRdd(extent_tile_rdd, FloatingLayoutScheme(TILE_SIZE))

    val tiled_rdd: RDD[(SpatialKey, MultibandTile)] =
      extent_tile_rdd
        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
        .repartition(RDD_PARTS)

    // ### Reproject
    val (zoom, reprojected_rdd): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
      MultibandTileLayerRDD(tiled_rdd, rasterMetaData)
        .reproject(CRS.fromEpsgCode(32651), FloatingLayoutScheme(TILE_SIZE), Bilinear)
    val final_crs = CRS.fromEpsgCode(32651)

    // Write tiles to dir
    //    reprojected_rdd.map{ tup =>
    //        val (spatial_key:SpatialKey, raster_tile:MultibandTile) = tup
    //        val extent : Extent = spatial_key.extent(rasterMetaData.layout)
    //        println("Indexing Spatial Key: "+spatial_key)
    //        //val gtif_file_path : String = output_dir_path+"_"+hilbert_hex+".tif"
    //        val gtif_file_path : String = os_path_join(tile_out_path,spatial_key._1+"_"+spatial_key._2+".tif")
    //
    //
    //        Holder.log.debug(s"Cutting $SpatialKey of ${raster_tile.dimensions} cells covering $extent to [$gtif_file_path]")
    //        // Write tile to GeoTiff file
    //        GeoTiff(raster_tile, extent, final_crs).write(gtif_file_path)
    //      }

    val collected_rdd = reprojected_rdd.collect()
    println("METRIC: sizeEstimate - collected_rdd: "+SizeEstimator.estimate(collected_rdd).toString)


    println("SPATIAL KEYS:")
    collected_rdd.foreach(
      { tup =>
        val (spatial_key:SpatialKey, raster_tile:MultibandTile) = tup
        println("SK: ["+spatial_key._1+"_"+spatial_key._2+"] ==> EXTENT:"+spatial_key.extent(rasterMetaData.layout))
      })


    return (reprojected_rdd, rasterMetaData)
  }

  def downloadSHPtoTempShpDir(hdfs_shp_path : String)(implicit sc : SparkContext, hdfs: fs.FileSystem): Unit ={
    // Check if temp dir for SHP files is present
    val tmp_shp_dir = new File(Constants.TMP_SHP_DIRECTORY)
    if (!tmp_shp_dir.exists) throw new Exception(s"Temporary Directory [${tmp_shp_dir}] Not Found!")

    val hdfs_shp_prefix = hdfs_shp_path.split('.').head + ".*"

    println(s"HDFS: Prefix [${hdfs_shp_prefix}]")

    val hdfs_shp_files = hdfs.listStatus(new fs.Path(hdfs_shp_prefix))

    hdfs_shp_files.map{ shp_file =>
      println(s"HDFS: Processing [${shp_file.getPath.toString}]")
      val src_path = shp_file.getPath()
      val dst_path = os_path_join(tmp_shp_dir.getPath, shp_file.getPath.getName)
      println(s"HDFS: Copying [${src_path}] to [${dst_path}]")
      hdfs.copyToLocalFile(src_path, dst_path)
    }
  }

  def simpleQueryTilesWithShp(tiled_rdd_meta: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], qry_shp_path : String, output_gtif_path : String )
                             (implicit sc: SparkContext, hdfs: fs.FileSystem): RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]] ={


//    val features = ShapeFileReader.readMultiPolygonFeaturesHDFS(qry_shp_path)
    val features = ShapeFileReader.readMultiPolygonFeatures(qry_shp_path)
    val region: MultiPolygon = features(0).geom
    val query_extents = features(0).envelope
    val attribute_table = features(0).data
    val filtered_rdd = tiled_rdd_meta.filter().where(Intersects(query_extents)).result

    val raster_tile: Raster[MultibandTile] = filtered_rdd.mask(region).stitch // Correct so far
    println("METRIC: Executor Memory "+sc.getExecutorMemoryStatus)
    println("METRIC: sizeEstimate - features: "+SizeEstimator.estimate(features).toString)
    println("METRIC: sizeEstimate - filtered_rdd: "+SizeEstimator.estimate(filtered_rdd).toString)

    if(output_gtif_path contains "hdfs:"){
      val hdfs_out = new BufferedOutputStream(hdfs.create(new fs.Path(output_gtif_path)))
      println(s"HDFS: Home Directory [${hdfs.getHomeDirectory}]")
      println(s"HDFS: Writing to [${output_gtif_path}]")
      val gtif_bytes = GeoTiff(raster_tile, tiled_rdd_meta.metadata.crs).toByteArray
      println(s"HDFS: Writing bytes of length [${gtif_bytes.length}]")
      hdfs_out.write(gtif_bytes)
      hdfs_out.close()
    }
    else {
      println(s"FILE: Writing to [${output_gtif_path}]")
      GeoTiff(raster_tile, tiled_rdd_meta.metadata.crs).write(output_gtif_path)
    }


    return filtered_rdd
  }

  def simpleQueryMetadataWithShp(tiled_rdd_meta: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]], meta_shp_path : String, output_json_path : String)
                                (implicit sc: SparkContext, hdfs: fs.FileSystem) {

    val metadata_fts: Seq[MultiPolygonFeature[Map[String, Object]]] = ShapeFileReader.readMultiPolygonFeatures(meta_shp_path)
    val metadata_fts_rdd: RDD[MultiPolygonFeature[Map[String, Object]]] = sc.parallelize(metadata_fts, RDD_PARTS)

    // Get extents and combine to a multipolygon
    val mapTransform = tiled_rdd_meta.metadata.getComponent[LayoutDefinition].mapTransform
    val tile_extents_mp = MultiPolygon(tiled_rdd_meta.map {
      case (k, tile) =>
        val key = k.getComponent[SpatialKey]
        mapTransform(key).toPolygon()
    }.collect())

    val json_serializer = Json(DefaultFormats)

    val meta_json_string : String = metadata_fts_rdd.aggregate[String]("")(
      {(acc, cur_ft) =>
        if(cur_ft.intersects(tile_extents_mp)){
          val map_json = Json(DefaultFormats).write(cur_ft.data)
          acc + ",\n" + map_json
        }else{
          acc
        }
      },
      {(acc1, acc2) =>
        if(acc1.length > 0 && acc2.length > 0) {
          acc1 + ",\n" + acc2
        }else{
          acc1 + acc2
        }
      })

    println("METADATA JSON:")
    pprint.pprintln(meta_json_string)

    if(output_json_path contains "hdfs:"){
      //TODO:write HDFS textfile
      val hdfs_out = new BufferedOutputStream(hdfs.create(new fs.Path(output_json_path)))
      println(s"HDFS: Home Directory [${hdfs.getHomeDirectory}]")
      println(s"HDFS: Writing metadata json to [${output_json_path}]")
      val json_bytes = meta_json_string.getBytes
      println(s"HDFS: Writing bytes of length [${json_bytes.length}]")
      hdfs_out.write(json_bytes)
      hdfs_out.close()
    }
    else{
      new PrintWriter(
        output_json_path)
      {
        write(meta_json_string + "\n"); close }
    }

  }

}
