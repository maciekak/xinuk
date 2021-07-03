package pl.edu.agh.xinuk.model.balancing

import pl.edu.agh.xinuk.model.{Cell, CellId, Direction, WorkerId}

import scala.collection.mutable
  
class CellsToChange (val workerId: WorkerId,
                     val cellsToRemove: Set[CellId],
                     val oldLocalCells: Set[CellId],
                     val cells: Map[CellId, Cell],
                     val incomingCells: Map[WorkerId, Set[CellId]],
                     val outgoingCells: mutable.Map[WorkerId, mutable.Set[CellId]],
                     val cellToWorker: Map[CellId, WorkerId],
                     val cellNeighbours: Map[CellId, Map[Direction, CellId]],
                     val remainingLocalCells: Set[CellId],
                     val borderOfRemainingCells: Set[CellId],
                     val outgoingCellsToRemove: Set[CellId],
                     val newIncomingCells: Set[CellId],
                     val newOutgoingCells: Set[CellId],
                     val neighboursOutgoingCellsToRemove: Map[WorkerId, Set[CellId]]){
}
