package pl.edu.agh.xinuk.balancing

import pl.edu.agh.xinuk.model.balancing.CellsToChange
import pl.edu.agh.xinuk.model.grid.{GridCellId, GridWorldShard}
import pl.edu.agh.xinuk.model.{Cell, CellId, Direction, WorkerId}

import scala.collection.mutable
import scala.collection.mutable.{Map => MutableMap, Set => MutableSet}

class BalancerAlgo(val worldShard: GridWorldShard,
                   var balancingNeighbours: Map[WorkerId, (Int, Int)],
                   val metricFun: ((Int, Int), GridCellId) => Double,
                   val shouldGoDepth: Boolean = false,
                   val shouldUseMetricOnAllFoundCells: Boolean = false,
                   val shouldUpdateMiddlePoint: Boolean = false,
                   val isAreaType: Boolean = false,
                   val slowAreaPlace: Set[CellId] = Set.empty) {

  val neighboursPlanAvgTime: collection.mutable.Map[WorkerId, StatisticCollector] =
    collection.mutable.Map.empty[WorkerId, StatisticCollector] ++
      balancingNeighbours.keys.map(w => w -> new StatisticCollector).toMap
      
  var slowAreaSize: Int = slowAreaPlace.count(c => worldShard.localCellIds.contains(c))
      
  def workerCurrentNeighbours: mutable.Set[WorkerId] = {
    mutable.Set.empty ++ worldShard.outgoingCells.keySet
  }

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
    
    if (shouldUpdateMiddlePoint) {
      //unefficient
      val size = worldShard.localCellIds.size
      val res = worldShard.localCellIds.foldLeft((0.0, 0.0))((v, cell) => {
        val grid = cell.asInstanceOf[GridCellId]
        (v._1 + grid.x, v._2 + grid.y)
      })
      val newPoint = ((res._1 / size).asInstanceOf[Int], (res._2 / size).asInstanceOf[Int])
      balancingNeighbours = balancingNeighbours + (worldShard.workerId -> newPoint)
    }
    if (isAreaType) {
      val newSize = localCellsIds.count(l => slowAreaPlace.contains(l))
      slowAreaSize += newSize
    }
    
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
        if(worldShard.cellNeighbours.contains(k)){
          worldShard.cellNeighbours(k) ++= v
        } else {
          worldShard.cellNeighbours += k -> (mutable.Map.empty ++ v)
        }
      } else {
        val availNeighbours = v.filter(item => updatedLocalCells.contains(item._2)).to(Map)
        if (worldShard.cellNeighbours.contains(k)) {
          worldShard.cellNeighbours(k) ++= availNeighbours
        } else {
          worldShard.cellNeighbours += (k -> availNeighbours.to(MutableMap))
        }
      }
    })
    
    (outCells, inCells)
  }

  def shrinkCells(cells: Map[CellId, Cell],
                  localCellsIds: Set[CellId],
                  oldLocalCells: Set[CellId],
                  workerId: WorkerId,
                  outgoingCellsToRemove: Set[CellId],
                  newIncomingCells: Set[CellId],
                  newOutgoingCells: Set[CellId],
                  borderOfRemainingCells: Set[CellId]): Unit = {

    worldShard.localCellIds --= localCellsIds
    
    if (shouldUpdateMiddlePoint) {
      //unefficient
      val size = worldShard.localCellIds.size
      val res = worldShard.localCellIds.foldLeft((0.0, 0.0))((v, cell) => {
        val grid = cell.asInstanceOf[GridCellId]
        (v._1 + grid.x, v._2 + grid.y)
      })
      val newPoint = ((res._1 / size).asInstanceOf[Int], (res._2 / size).asInstanceOf[Int])
      balancingNeighbours = balancingNeighbours + (worldShard.workerId -> newPoint)
    }
    if (isAreaType) {
      val newSize = localCellsIds.count(l => slowAreaPlace.contains(l))
      slowAreaSize -= newSize
    }
    
    worldShard.cells --= outgoingCellsToRemove

    worldShard.cellToWorker --= outgoingCellsToRemove
    worldShard.cellToWorker ++= oldLocalCells.map(c => c -> workerId)

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
    if(newOutgoingCells.nonEmpty && worldShard.outgoingCells.contains(oldNeighbour)) {
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
      if(intersectCells.nonEmpty){
        if(worldShard.outgoingCells.contains(newNeighbour)){
          worldShard.outgoingCells(newNeighbour) ++= intersectCells
        } else {
          worldShard.outgoingCells += newNeighbour -> (MutableSet.empty ++ intersectCells)
        }
      }
    } else {
      diffOutCells = newOutgoingCells
    }

    var diffNewInCells: Set[CellId] = Set.empty
    var diffInCells: Set[CellId] = Set.empty
    if((incomingCellsToRemove.nonEmpty || newIncomingCells.nonEmpty) && worldShard.incomingCells.contains(oldNeighbour)){
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

    renewWorkerNeighbour()
    (diffNewInCells, diffInCells, diffOutCells)
  }

  def findCells(workerId: WorkerId, quantity: Int): CellsToChange = {

//    val mask = balancingNeighbours(worldShard.workerId)
    //uncomment line below to make tests work
        val mask = balancingNeighbours(workerId)
    val gridCells = mutable.Set.empty[GridCellId] ++ worldShard.incomingCells(workerId).asInstanceOf[mutable.Set[GridCellId]] 
    val cellsToRemove = if (shouldGoDepth) takeMaxNCellsInDepth(gridCells, mask, quantity)
                        else takeMaxNCells(gridCells, mask, quantity)

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
      && item._2.values.exists(c => remainingLocalCells.contains(c) || !cellsToRemove.contains(c) && worldShard.localCellIds.contains(c)))
      .keys
      .to(Set)
    val oldLocalCells = cellsToRemove.intersect(borderOfRemainingCells)
    val outgoingCellsToRemove = cellsKeys.filterNot(c => 
      remainingLocalCells.contains(c) 
        || borderOfRemainingCells.contains(c)).to(Set)
    val newIncomingCells = remainingLocalCells.filter(cellId => cellNeighbours(cellId).values.exists(c => cellsToRemove.contains(c)))
    val newOutgoingCells = cellsToRemove.filter(cellId => cellNeighbours(cellId).values.exists(c => remainingLocalCells.contains(c)))
    
    val neighboursOutgoingCellsToRemove = outgoingCellsToRemove.groupBy(c => cellToWorker(c)) - worldShard.workerId

    new CellsToChange(workerId,
      cellsToRemove,
      oldLocalCells,
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

  private def takeMaxNCells(cells: MutableSet[GridCellId], mask: (Int, Int), quantityParam: Int): Set[CellId] = {
    var quantity = quantityParam
    if (quantity >= cells.size) {
      if(quantity >= worldShard.localCellIds.size) {
        quantity = worldShard.localCellIds.size - 1
      } else {
        return cells.toSet
      }
    }

    val ordering = Ordering[Double].reverse
    cells.toSeq.sortBy(c => metricFun(mask, c))(ordering).take(quantity).toSet
  }

  private def takeMaxNCellsInDepth(cellsParam: MutableSet[GridCellId], mask: (Int, Int), quantityParam: Int): Set[CellId] = {
    var cells = cellsParam
    if(quantityParam <= cells.size) takeMaxNCells(cells, mask, quantityParam)
    var quantity = quantityParam
    if (quantity >= worldShard.localCellIds.size) {
      quantity = worldShard.localCellIds.size - 1
    }

    var takenCells = mutable.Set.empty[GridCellId]
    while(takenCells.size + cells.size < quantity) {
      takenCells ++= cells
      cells = cells.flatMap(c => worldShard.cellNeighbours(c).values.filter(n => {
        val gridCellId = n.asInstanceOf[GridCellId]
        !takenCells.contains(gridCellId) && worldShard.localCellIds.contains(gridCellId)
      }).toSet.asInstanceOf[Set[GridCellId]])
    }

    if(quantity == takenCells.size + cells.size){
      return (takenCells ++ cells).toSet
    }
    
    val cellsToCalc = if (shouldUseMetricOnAllFoundCells) cells ++ takenCells else cells

    val withValue = cellsToCalc.map(c => c -> metricFun(mask, c)).toSeq
    val sortedCells = withValue.sortBy(_._2).reverse.map(_._1)
    takenCells = if (shouldUseMetricOnAllFoundCells) mutable.Set.empty ++ sortedCells.take(quantity).toSet
                 else takenCells ++ sortedCells.take(quantity - takenCells.size)
    takenCells.toSet.asInstanceOf[Set[CellId]]
  }

  private def renewWorkerNeighbour() = {
    worldShard.outgoingWorkerNeighbours.clear()
    worldShard.outgoingWorkerNeighbours ++= (worldShard.outgoingCells.keySet.toSet + worldShard.workerId)
    worldShard.incomingWorkerNeighbours.clear()
    worldShard.incomingWorkerNeighbours ++= (worldShard.incomingCells.keySet.toSet + worldShard.workerId)
  }
}
