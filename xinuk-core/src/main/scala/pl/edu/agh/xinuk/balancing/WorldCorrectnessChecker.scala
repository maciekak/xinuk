package pl.edu.agh.xinuk.balancing

import org.slf4j.Logger
import pl.edu.agh.xinuk.config.XinukConfig
import pl.edu.agh.xinuk.model.{Cell, CellId, Direction, WorkerId, WorldShard}
import pl.edu.agh.xinuk.model.grid.{GridCellId, GridDirection, GridWorldShard}

import scala.collection.mutable

object WorldCorrectnessChecker {
  private var shardRef: Map[WorkerId, GridWorldShard] = Map.empty
  private var workerShard: Map[WorkerId, mutable.Set[GridCellId]] = Map.empty
  private var lastIteration: Long = 0
  private var logger: Logger = _
  private var config: XinukConfig = _
  
  private val changesForIteration: mutable.Map[Long, Map[WorkerId, (WorkerId, Set[GridCellId])]] = mutable.Map.empty.withDefaultValue(Map.empty)
  
  def initShards(shard: WorldShard, logger: Logger, config: XinukConfig): Unit = {
    val worldShard = shard.asInstanceOf[GridWorldShard]
    val cells = shard.localCellIds.map(l => l.asInstanceOf[GridCellId])
    synchronized {
      workerShard += shard.workerId -> cells
      shardRef += shard.workerId -> worldShard
      this.logger = logger
      this.config = config
    }
  }
  
  def addChanges(changes: (WorkerId, WorkerId, Set[CellId]), iteration: Long): Unit = {
    val mapped = changes._3.map(c => c.asInstanceOf[GridCellId])
    val tuple = (changes._2, mapped)
    synchronized {
      changesForIteration(iteration) += (changes._1 -> tuple)
    }
  }
  
  def checkIteration(iteration: Long): Unit = {
    synchronized {
      if (iteration == lastIteration) {
        return
      }
      
      lastIteration = iteration
    }

    changesForIteration(iteration).foreach(item => {
      logger.info("It: " + iteration + " " + item._1 + " -> " + item._2._1 + ": " + item._2._2.size)
      workerShard(item._1) --= item._2._2
      workerShard(item._2._1) ++= item._2._2
    })
    
    val shards = generateWorldShard(workerShard.map(i => i._1 -> i._2.toSet))
    shards.foreach(sh => {
      checkEqualWorldShard(sh, shardRef(sh.workerId))
    })
  }

  private def generateWorldShard(setOfGridCells: Map[WorkerId, Set[GridCellId]]): Set[GridWorldShard] = {
    setOfGridCells.map(w => {
      val border = w._2.flatMap(g => {
        val neighbours = GridDirection.values.map(v => v.of(g))
        setOfGridCells.map(item => item._1 -> item._2.filter(i => neighbours.contains(i) && item._1 != w._1))
          .filter(item => item._2.nonEmpty)
          .toSet
      })
        .groupBy(w => w._1)
        .map(k => k._1 -> k._2.flatMap(l => l._2))
      createWorldShard(w._2, border, w._1)
    }).toSet
  }

  private def createWorldShard(localCells: Set[GridCellId],
                       border: Map[WorkerId, Set[GridCellId]],
                       workerId: WorkerId): GridWorldShard = {
    val borderCells = border.values.flatten.toSet
    val cellsKeys = localCells ++ borderCells
    val cells = cellsKeys.map(c => c -> Cell.empty(c)(config)).toMap
    val cellToWorker = localCells.map(l => l -> workerId).toMap ++ border.flatMap(b => b._2.map(c => c -> b._1))
    val cellNeighbours = (localCells.map(l => {
      val neighbours = (for
        (direction: GridDirection <- GridDirection.values;
         to = direction.of(l)
         if cellsKeys.contains(to))
        yield direction -> to).toMap
      l -> neighbours
    }) ++ borderCells.map(b => {
      val neighbours = (for
        (direction <- GridDirection.values;
         to = direction.of(b)
         if localCells.contains(to))
        yield direction -> to).toMap
      b -> neighbours
    })).toMap
    val incomingCells = border.map(b => b._1 -> b._2.flatMap(c => cellNeighbours(c).values))
    val castedCellNeighbours = mutable.Map.empty ++ cellNeighbours.map(c => c._1 -> (mutable.Map.empty ++ c._2.asInstanceOf[Map[Direction, CellId]])).asInstanceOf[Map[CellId, mutable.Map[Direction, CellId]]]
    val castedBorder = mutable.Map.empty ++ border.map(b => b._1 -> (mutable.Set.empty ++ b._2.asInstanceOf[Set[CellId]]))
    val castedIncomingCells = mutable.Map.empty ++ incomingCells.map(i => i._1 -> (mutable.Set.empty ++ i._2.asInstanceOf[Set[CellId]]))
    new GridWorldShard(
      mutable.Map.empty ++ cells.asInstanceOf[Map[CellId, Cell]],
      castedCellNeighbours,
      workerId,
      castedBorder,
      castedIncomingCells,
      mutable.Map.empty ++ cellToWorker.asInstanceOf[Map[CellId, WorkerId]])(config)
  }

  private def checkEqualWorldShard(first: GridWorldShard, second: GridWorldShard): Unit = {
    var allOk = true
      if(!first.cells.keySet.equals(second.cells.keySet)) {
        logger.info("Cells of " + first.workerId.toString + " unequal in iteration: " + lastIteration + "\n" + "first: " + first.cells.keySet.toString() + "\nsecond: " + second.cells.keySet.toString())
        allOk = false
      }
    if(!first.localCellIds.equals(second.localCellIds)) {
      logger.info("LocalCells of " + first.workerId.toString + " unequal in iteration: " + lastIteration + "\n" + "first: " + first.localCellIds.toString() + "\nsecond: " + second.localCellIds.toString())
    }
    if(!first.cellNeighbours.keySet.equals(second.cellNeighbours.keySet) ||
      !first.cellNeighbours.forall(c => c._2.values.toSet.equals(second.cellNeighbours(c._1).values.toSet))) {
      logger.info("CellsNeigh of " + first.workerId.toString + " unequal in iteration: " + lastIteration + "\n" + "first: " + first.cellNeighbours.toString() + "\nsecond: " + second.cellNeighbours.toString())
      allOk = false
    }
    if(!first.incomingCells.keySet.equals(second.incomingCells.keySet) ||
      !first.incomingCells.forall(c => c._2.equals(second.incomingCells(c._1)))) {
      logger.info("incomingCells of " + first.workerId.toString + " unequal in iteration: " + lastIteration + "\n" + "first: " + first.incomingCells.toString() + "\nsecond: " + second.incomingCells.toString())
      allOk = false
    }
    if(!first.outgoingCells.keySet.equals(second.outgoingCells.keySet) ||
      !first.outgoingCells.forall(c => c._2.equals(second.outgoingCells(c._1)))) {
      logger.info("outgoingCells of " + first.workerId.toString + " unequal in iteration: " + lastIteration + "\n" + "first: " + first.outgoingCells.toString() + "\nsecond: " + second.outgoingCells.toString())
      allOk = false
    }
    if(!first.cellToWorker.keySet.equals(second.cellToWorker.keySet) ||
      !first.cellToWorker.forall(c => c._2.equals(second.cellToWorker(c._1)))) {
      logger.info("cellToWorker of " + first.workerId.toString + " unequal in iteration: " + lastIteration + "\n" + "first: " + first.cellToWorker.toString() + "\nsecond: " + second.cellToWorker.toString())
      allOk = false
    }
    
    if(allOk) {
      logger.info(first.workerId.toString + " is OK")
    }
  }
}
