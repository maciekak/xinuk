package pl.edu.agh.xinuk.simulation

import pl.edu.agh.xinuk.model.balancing.CellsToChange
import pl.edu.agh.xinuk.model.grid.{GridCellId, GridWorldShard}
import pl.edu.agh.xinuk.model.{Cell, CellId, Direction, WorkerId}

import scala.collection.mutable.{Map => MutableMap, Set => MutableSet}

class BalancerAlgo(val worldShard: GridWorldShard,
                   val balancingNeighbours: Map[WorkerId, (Int, Int)],
                   val metricFun: ((Int, Int), GridCellId) => Double) {

  val neighboursPlanAvgTime: collection.mutable.Map[WorkerId, Double] =
    collection.mutable.Map.empty[WorkerId, Double] ++
      balancingNeighbours.keys.map(w => w -> 0.0).toMap


  def expandCells(cells: Map[CellId, Cell],
                  localCellsIds: Set[CellId],
                  workerId: WorkerId,
                  incomingCells: Map[WorkerId, Set[CellId]],
                  incomingCellsToRemove: Set[CellId],
                  newIncomingCells: Set[CellId],
                  cellToWorker: Map[CellId, WorkerId],
                  cellNeighbours: Map[CellId, Map[Direction, CellId]]): (
      Map[WorkerId, Set[CellId]],
      Map[WorkerId, Set[CellId]]) = {

    worldShard.localCellIds ++= localCellsIds
    worldShard.cells ++= cells
    
    val keysSet = cells.keySet
    val notNewLocalCells = keysSet -- localCellsIds
    val notLocalCells = notNewLocalCells.diff(worldShard.localCellIds)
    val updatedLocalCells = keysSet -- notLocalCells
    val outCells = notLocalCells.groupBy(c => cellToWorker(c))
    outCells.foreachEntry((k, v) => {
      if (worldShard.outgoingCells.contains(k)) {
        worldShard.outgoingCells(k) ++= v
      } else {
        worldShard.outgoingCells += (k -> v.to(MutableSet))
      }
    })
    val workerOutgoing = worldShard.outgoingCells(workerId)
    workerOutgoing --= localCellsIds
    if (workerOutgoing.isEmpty) {
      worldShard.outgoingCells -= workerId
    }

    val workerIncoming = worldShard.incomingCells(workerId)
    workerIncoming --= incomingCellsToRemove
    workerIncoming ++= newIncomingCells
    if (workerIncoming.isEmpty) {
      worldShard.incomingCells -= workerId
    }
    val inCells = incomingCells - worldShard.workerId
    inCells.foreachEntry((k, v) => {
      if (worldShard.incomingCells.contains(k)) {
        worldShard.incomingCells(k) ++= v
      } else {
        worldShard.incomingCells += (k -> v.to(MutableSet))
      }
    })

    renewWorkerNeighbour()

    worldShard.cellToWorker ++= cellToWorker
    worldShard.cellToWorker ++= localCellsIds.map(c => c -> worldShard.workerId)

    cellNeighbours.foreachEntry((k, v) => {
      if (updatedLocalCells.contains(k)) {
        worldShard.cellNeighbours(k) ++= v
      } else {
        val availNeighbours = v.filter(item => updatedLocalCells.contains(item._2)).to(Map)
        if (worldShard.cellNeighbours.contains(k)) {
          worldShard.cellNeighbours(k) ++= availNeighbours
        } else {
          worldShard.cellNeighbours += (k -> availNeighbours.to(MutableMap))
        }
      }
    })
    
    //TODO: do przemyślenia jak styk nowego i starego obszaru
    (outCells, inCells)
  }

  def shrinkCells(cells: Map[CellId, Cell],
                  localCellsIds: Set[CellId],
                  workerId: WorkerId,
                  outgoingCellsToRemove: Set[CellId],
                  newIncomingCells: Set[CellId],
                  newOutgoingCells: Set[CellId],
                  borderOfRemainingCells: Set[CellId]): Unit = {
    
    worldShard.localCellIds --= localCellsIds
    worldShard.cells --= outgoingCellsToRemove

    worldShard.cellToWorker --= outgoingCellsToRemove
    worldShard.cellToWorker ++= localCellsIds.map(c => c -> workerId)

    worldShard.cellNeighbours --= outgoingCellsToRemove
    borderOfRemainingCells.foreach(id => {
      val notAvailNeighbours = worldShard.cellNeighbours(id).filter(item => outgoingCellsToRemove.contains(item._2) || borderOfRemainingCells.contains(item._2)).keys
      worldShard.cellNeighbours(id) --= notAvailNeighbours
    })

    if(newOutgoingCells.nonEmpty) {
      worldShard.outgoingCells(workerId) ++= newOutgoingCells
    }
    worldShard.outgoingCells --= worldShard.outgoingCells.map(item => {
      item._2 --= outgoingCellsToRemove
      item._1 -> item._2.size
    })
      .filter(item => item._2 == 0)
      .keys

    if (newIncomingCells.nonEmpty) {
      worldShard.incomingCells(workerId) ++= newIncomingCells
    }
    worldShard.incomingCells --= worldShard.incomingCells.map(item => {
      item._2 --= localCellsIds
      item._1 -> item._2.size
    })
      .filter(item => item._2 == 0)
      .keys

    renewWorkerNeighbour()
  }
  
  def fixNeighbourhood(newNeighbour: WorkerId, 
                       oldNeighbour: WorkerId,
                       newIncomingCells: Set[CellId],
                       incomingCellsToRemove: Set[CellId],
                       newOutgoingCells: Set[CellId])
                  :(Set[CellId], Set[CellId], Set[CellId]) = {
    var diffOutCells: Set[CellId] = Set.empty
    if(worldShard.outgoingCells.contains(oldNeighbour)) {
      val oldOutCells = worldShard.outgoingCells(oldNeighbour)
      val intersectCells = newOutgoingCells.intersect(oldOutCells)
      if(intersectCells.size != newOutgoingCells.size){
        diffOutCells = newOutgoingCells.diff(intersectCells)
      }
      worldShard.cellToWorker ++= intersectCells.map(c => c -> newNeighbour)
      oldOutCells --= intersectCells
      if(oldOutCells.isEmpty){
        worldShard.outgoingCells -= oldNeighbour
      }
      if(worldShard.outgoingCells.contains(newNeighbour)){
        worldShard.outgoingCells(newNeighbour) ++= intersectCells
      } else {
        worldShard.outgoingCells += newNeighbour -> (MutableSet.empty ++ intersectCells)
      }
    } else {
      diffOutCells = newOutgoingCells
    }
    
    var diffNewInCells: Set[CellId] = Set.empty
    var diffInCells: Set[CellId] = Set.empty
    if(worldShard.incomingCells.contains(oldNeighbour)){
      val oldInCells = worldShard.incomingCells(oldNeighbour)
      val intersectCells = incomingCellsToRemove.intersect(oldInCells)
      if(intersectCells.size != incomingCellsToRemove.size){
        diffInCells = incomingCellsToRemove.diff(intersectCells)
      }
      oldInCells --= intersectCells
      if(oldInCells.isEmpty){
        worldShard.incomingCells -= oldNeighbour
      }
      diffNewInCells = newIncomingCells.diff(worldShard.localCellIds)
      val newLocalIncomingCells = newIncomingCells.diff(diffNewInCells)
      if(worldShard.incomingCells.contains(newNeighbour)){
        worldShard.incomingCells(newNeighbour) ++= newLocalIncomingCells
      } else if(newLocalIncomingCells.nonEmpty) {
        worldShard.incomingCells += newNeighbour -> (MutableSet.empty ++ newLocalIncomingCells)
      }
    } else {
      diffNewInCells = newIncomingCells
      diffInCells = incomingCellsToRemove
    }
    
    (diffNewInCells, diffInCells, diffOutCells)
  }

  def findCells(workerId: WorkerId, quantity: Int): CellsToChange = {

    val mask = balancingNeighbours(workerId)
    val gridCells = worldShard.incomingCells(workerId).toSeq.asInstanceOf[Seq[GridCellId]]
    val cellsToRemove = takeMaxNCells(gridCells, mask, quantity).to(Set)

    val cells = cellsToRemove
      .flatMap(c => worldShard.cellNeighbours(c).values)
      .map(c => c -> worldShard.cells(c))
      .to(Map) ++ cellsToRemove.map(id => id -> worldShard.cells(id))
    val cellsKeys = cells.keySet

    val incomingCells = worldShard.incomingCells
      .map(item => item._1 -> item._2.intersect(cellsToRemove).toSet)
      .filter(c => c._2.nonEmpty)
      .toMap
    
    val outgoingCells = worldShard.outgoingCells
      .map(item => item._1 -> item._2.intersect(cellsKeys))
      .filter(c => c._2.nonEmpty)

    val cellToWorker = cellsKeys
      .map(c => c -> worldShard.cellToWorker(c))
      .to(Map)

    val cellNeighbours = cellsKeys
      .map(c => c -> worldShard.cellNeighbours(c).toMap)
      .to(Map)

    val remainingLocalCells = cellsKeys.filter(c => !cellsToRemove.contains(c) && worldShard.localCellIds.contains(c)).to(Set)
    val borderOfRemainingCells = cellNeighbours.filter(item => !remainingLocalCells.contains(item._1)
      && item._2.values.exists(c => remainingLocalCells.contains(c)))
      .keys
      .to(Set)
    val outgoingCellsToRemove = cellsKeys.filterNot(c => remainingLocalCells.contains(c) || borderOfRemainingCells.contains(c)).to(Set)
    val newIncomingCells = remainingLocalCells.filter(cellId => cellNeighbours(cellId).values.exists(c => cellsToRemove.contains(c)))
    val newOutgoingCells = cellsToRemove.filter(cellId => cellNeighbours(cellId).values.exists(c => remainingLocalCells.contains(c)))
    
    val neighboursOutgoingCellsToRemove = outgoingCellsToRemove.groupBy(c => cellToWorker(c)) - worldShard.workerId

    new CellsToChange(workerId,
      cellsToRemove,
      cells,
      incomingCells,
      outgoingCells,
      cellToWorker,
      cellNeighbours,
      remainingLocalCells,
      borderOfRemainingCells,
      outgoingCellsToRemove,
      newIncomingCells,
      newOutgoingCells,
      neighboursOutgoingCellsToRemove
    )
  }

  private def getMetricValue(mask: (Int, Int), cellId: GridCellId): Int = {
    cellId.x * mask._1 + cellId.y * mask._2
  }

  private def takeMaxNCells(cells: Seq[GridCellId], mask: (Int, Int), quantity: Int): Seq[CellId] = {
    if (quantity >= cells.size) {
      return cells
    }

    cells.sortBy(c => metricFun(mask, c))
      .take(quantity)
  }

  private def renewWorkerNeighbour() = {
    worldShard.outgoingWorkerNeighbours.clear()
    worldShard.outgoingWorkerNeighbours ++= (worldShard.outgoingCells.keySet.toSet + worldShard.workerId)
    worldShard.incomingWorkerNeighbours.clear()
    worldShard.incomingWorkerNeighbours ++= (worldShard.incomingCells.keySet.toSet + worldShard.workerId)
  }
}
