package simpletiler

object Constants {
  val TILE_SIZE = 1024 //in pixels
  val GRID_CELL_WIDTH = 10.0
  val GRID_CELL_HEIGHT = 10.0

  //val RDD_PARTS = 50
  val RDD_PARTS = 40
  val PAD_LENGTH = 8
  val SFC_LABEL_HILBERT="hilbert"
  val SFC_LABEL_ZORDER="zorder"
  val TMP_DIRECTORY="/tmp/simpletiler"
  val TMP_SHP_DIRECTORY="/tmp/simpletiler/shp"
}
