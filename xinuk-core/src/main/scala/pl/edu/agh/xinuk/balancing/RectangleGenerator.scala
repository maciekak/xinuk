package pl.edu.agh.xinuk.balancing

import pl.edu.agh.xinuk.model.CellId
import pl.edu.agh.xinuk.model.grid.GridCellId

object RectangleGenerator {
  def get(x1: Int, y1: Int, x2: Int, y2: Int): Set[CellId] = {
    (for {
      x <- x1 until x2+1
      y <- y1 until y2+1
    } yield GridCellId(x, y)).toSet
  }
}
