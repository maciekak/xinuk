package pl.edu.agh.xinuk.balancing

import pl.edu.agh.xinuk.model.grid.GridCellId

object MetricFunctions {
  def middlePointFar(point: (Int, Int), cellId: GridCellId): Double = {
    -middlePointClose(point, cellId)
  }

  def middlePointClose(point: (Int, Int), cellId: GridCellId): Double = {
    math.pow(point._1 - cellId.x, 2) + math.pow(point._2 - cellId.y, 2)
  }

  def middlePointMaxMin(point: (Int, Int), cellId: GridCellId): Double = {
    //TODO: implement this
    math.pow(point._1 - cellId.x, 2) + math.pow(point._2 - cellId.y, 2)
  }
}
