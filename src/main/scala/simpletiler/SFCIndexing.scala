package simpletiler

import com.google.uzaygezen.core.{BitVectorFactories, CompactHilbertCurve, MultiDimensionalSpec}
import geotrellis.raster.TileLayout
import geotrellis.raster.io.geotiff.MultibandGeoTiff
import geotrellis.spark.{KeyBounds, SpatialKey}
import geotrellis.spark.io.index.KeyIndex
import geotrellis.spark.io.index.hilbert.HilbertSpatialKeyIndex
import geotrellis.spark.io.index.zcurve.{Z2, ZSpatialKeyIndex}
import geotrellis.spark.tiling.LayoutDefinition
import geotrellis.vector.Extent
import Constants.TILE_SIZE
import scala.collection.JavaConverters._


object SFCIndexing {
  def createSpatialKey[D](tile_extents: Extent, tile_layout: TileLayout, layout_def: LayoutDefinition): Seq[SpatialKey] = {
    val gridBounds = layout_def.mapTransform(tile_extents)

    for {
      (c, r) <- gridBounds.coords
      if r < tile_layout.totalRows
      if c < tile_layout.totalCols
    } yield SpatialKey(c, r)
  }

  def createSpatialKeyFromTiff(multiband_gtiff: MultibandGeoTiff): Seq[SpatialKey] = {
    val col_count = tile_ceiling_count(multiband_gtiff.extent.width)
    val row_count = tile_ceiling_count(multiband_gtiff.extent.height)
    val tile_layout = TileLayout(col_count, row_count, TILE_SIZE, TILE_SIZE)
    val layout_def = LayoutDefinition(multiband_gtiff.extent, tile_layout)

    //TODO: replace with Quad Tree indexed spatial keys
    return createSpatialKey(multiband_gtiff.extent, tile_layout, layout_def)
  }

  def createSFCIndex(sfc_index_label: String, x_resolution: Int, y_resolution: Int): KeyIndex[SpatialKey] = {

    sfc_index_label match {
      case Constants.SFC_LABEL_HILBERT => {
        val max_int = (math.pow(2, 32) - 1).toInt
        //new HilbertSpatialKeyIndex(KeyBounds(SpatialKey(0, 0), SpatialKey(max, max)), 31, 31)
        println("sfc_index: HILBERT : "+sfc_index_label)

        new HilbertSpatialKeyIndex(
          KeyBounds(
            SpatialKey(0, 0),
            //SpatialKey(max_int, max_int)),
            SpatialKey(max_int, max_int)),
          x_resolution,
          y_resolution)
      }
      case Constants.SFC_LABEL_ZORDER => {
        val max_int = (math.pow(2, 32) - 1).toInt
        println("sfc_index: ZORDER : "+sfc_index_label)

        new ZSpatialKeyIndex(
          KeyBounds(
            SpatialKey(0, 0),
            SpatialKey(x_resolution, y_resolution)))
      }
    }
  }

  def invertHexIndex(hex_string :String, x_resolution: Int, y_resolution: Int, sfc_index_label: String): SpatialKey = {
    val long_index = java.lang.Long.parseLong(hex_string, 16)

    sfc_index_label match {
      case Constants.SFC_LABEL_ZORDER => {
        return SpatialKey(Z2.combine(long_index), (Z2.combine(long_index>>1)))
      }
      case Constants.SFC_LABEL_HILBERT => {
        val chc = {
          val dimensionSpec =
            new MultiDimensionalSpec(
              List(
                x_resolution+1,
                y_resolution+1
              ).map(new java.lang.Integer(_)).asJava
            )

          new CompactHilbertCurve(dimensionSpec)
        }
        val bit_vectors =
          Array(
            BitVectorFactories.OPTIMAL.apply(x_resolution+1),
            BitVectorFactories.OPTIMAL.apply(y_resolution+1)
          )
        val long_var = java.lang.Long.parseLong(hex_string, 16)
        val hilbertBitVector = BitVectorFactories.OPTIMAL.apply(chc.getSpec.sumBitsPerDimension)
        hilbertBitVector.copyFrom(long_var)
        chc.indexInverse(hilbertBitVector, bit_vectors)

        // TODO: ??? Gets flipped for some reason ???
        //        return SpatialKey(bit_vectors(0).toLong.toInt ,bit_vectors(1).toLong.toInt)
        return SpatialKey(bit_vectors(1).toLong.toInt ,bit_vectors(0).toLong.toInt)
      }
      case _ => throw new Exception("Unhandled Index Label")
    }

  }

  def tile_ceiling_count(x: Double): Int = {
    return Math.ceil(x/TILE_SIZE.toDouble).toInt
  }

}