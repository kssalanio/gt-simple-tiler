package simpletiler

import geotrellis.proj4.CRS
import geotrellis.raster.MultibandTile
import geotrellis.raster.resample.Bilinear
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.{Metadata, MultibandTileLayerRDD, SpatialKey, TileLayerMetadata}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SimpleTilingFramework {
  def simpleReadTileQuery(run_rep: Int, src_raster_file_path: String, tile_out_path: String, meta_shp_path : String, qry_shp_path : String, output_gtif_path : String )
                         (implicit sc: SparkContext): Unit = {
    println("SIMPLE RUN REP: " + run_rep.toString)

    val (reprojected_rdd: RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]],
    raster_metadata) = simpleReadGeotiffAndTile(src_raster_file_path, tile_out_path)
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

    println("SPATIAL KEYS:")
    reprojected_rdd.collect().foreach(
      { tup =>
        val (spatial_key:SpatialKey, raster_tile:MultibandTile) = tup
        println("   ["+spatial_key._1+"_"+spatial_key._2+"]")
      })


    //    return (tiled_rdd, rasterMetaData)
    return (reprojected_rdd, rasterMetaData)
  }
}
