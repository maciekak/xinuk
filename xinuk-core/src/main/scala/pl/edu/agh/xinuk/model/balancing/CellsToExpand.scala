package pl.edu.agh.xinuk.model.balancing

import pl.edu.agh.xinuk.model.{Cell, CellId, Direction, WorkerId}

class CellsToExpand (val cells: Map[CellId, Cell],
                     val localCellsToChange: Set[CellId],
                     val workerId: WorkerId,
                     val incomingCells: Map[WorkerId, Set[CellId]],
                     val incomingCellsToRemove: Set[CellId],
                     val newIncomingCells: Set[CellId],
                     val cellToWorker: Map[CellId, WorkerId],
                     val cellNeighbours: Map[CellId, Map[Direction, CellId]],
                     val neighboursOutgoingCellsToRemove: Map[WorkerId, Set[CellId]]) {

}
