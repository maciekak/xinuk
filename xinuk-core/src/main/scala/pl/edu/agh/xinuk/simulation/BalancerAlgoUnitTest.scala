package pl.edu.agh.xinuk.simulation

import org.scalamock.scalatest.MockFactory
import org.scalatest.funsuite._
import pl.edu.agh.xinuk.config.XinukConfig
import pl.edu.agh.xinuk.model.grid.{GridCellId, GridDirection, GridWorldShard}
import pl.edu.agh.xinuk.model._

import scala.collection.immutable.{Map => ImMap, Set => ImSet}
import scala.collection.mutable.{Map => MutMap, Set => MutSet}

class BalancerAlgoUnitTest extends AnyFunSuite with MockFactory {
  val balancingNeighbours: Map[WorkerId, (Int, Int)] = Map(WorkerId(1) -> (1, 1), WorkerId(2) ->(1, 1))
  val worldType = mock[WorldType]
  (() => worldType.directions).expects().returning(GridDirection.values).anyNumberOfTimes()
  val config = mock[XinukConfig]
  (() => config.worldType).expects().returning(worldType).anyNumberOfTimes()
  
  def getMetricFun(gridCells: Set[GridCellId]): ((Int, Int), GridCellId) => Double = {
    (_: (Int, Int), cell: GridCellId) => {
      if (gridCells.contains(cell)) 1.0 else 0.0
    }
  }
  
  def getSquare(x: Int, y: Int, len: Int): ImSet[GridCellId] = {
    (for (l <- 0 until len * len) yield GridCellId(x + l % len, y + l / len)).toSet
  }
  
  def generateWorldShard(setOfGridCells: ImMap[WorkerId, ImSet[GridCellId]]): ImSet[GridWorldShard] = {
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

  def checkEqualWorldShard(first: GridWorldShard, second: GridWorldShard): Boolean = {
    first.workerId == second.workerId &&
      first.cells.keySet.equals(second.cells.keySet) &&
      first.cellNeighbours.keySet.equals(second.cellNeighbours.keySet) &&
      first.cellNeighbours.forall(c => c._2.values.toSet.equals(second.cellNeighbours(c._1).values.toSet)) &&
      first.incomingCells.keySet.equals(second.incomingCells.keySet) &&
      first.incomingCells.forall(c => c._2.equals(second.incomingCells(c._1))) &&
      first.outgoingCells.keySet.equals(second.outgoingCells.keySet) &&
      first.outgoingCells.forall(c => c._2.equals(second.outgoingCells(c._1))) &&
      first.cellToWorker.keySet.equals(second.cellToWorker.keySet) &&
      first.cellToWorker.forall(c => c._2.equals(second.cellToWorker(c._1))) &&
      first.localCellIds.equals(second.localCellIds)
  }
  
  def createWorldShard(localCells: ImSet[GridCellId], 
                       border: ImMap[WorkerId, ImSet[GridCellId]], 
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
    val castedCellNeighbours = MutMap.empty ++ cellNeighbours.map(c => c._1 -> (MutMap.empty ++ c._2.asInstanceOf[Map[Direction, CellId]])).asInstanceOf[Map[CellId, MutMap[Direction, CellId]]]
    val castedBorder = MutMap.empty ++ border.map(b => b._1 -> (MutSet.empty ++ b._2.asInstanceOf[Set[CellId]]))
    val castedIncomingCells = MutMap.empty ++ incomingCells.map(i => i._1 -> (MutSet.empty ++ i._2.asInstanceOf[Set[CellId]])) 
    new GridWorldShard(
      MutMap.empty ++ cells.asInstanceOf[Map[CellId, Cell]],
      castedCellNeighbours, 
      workerId,
      castedBorder,
      castedIncomingCells, 
      MutMap.empty ++ cellToWorker.asInstanceOf[Map[CellId, WorkerId]])(config)
  }

  test("test if all methods of unit test work correctly") {
    val localCells1 = ImSet(GridCellId(0,0), GridCellId(0,1), GridCellId(1,1), GridCellId(1,0))
    val border1 = ImMap(WorkerId(2) -> ImSet(GridCellId(0,2), GridCellId(1,2)))

    val ws1 = createWorldShard(localCells1, border1, WorkerId(1))
    val ws2 = createWorldShard(localCells1, border1, WorkerId(1))

    assert(checkEqualWorldShard(ws1, ws2))
  }

  test("test if all methods of unit test work correctly v2") {
    val localCells1 = ImSet(GridCellId(0,0), GridCellId(0,1), GridCellId(1,1), GridCellId(1,0))
    val border1 = ImMap(WorkerId(2) -> ImSet(GridCellId(0,2), GridCellId(1,2)))
    val localCells2 = ImSet(GridCellId(0,2), GridCellId(0,3), GridCellId(1,3), GridCellId(1,2))
    val border2 = ImMap(WorkerId(1) -> ImSet(GridCellId(0,1), GridCellId(1,1)))

    val ws1 = createWorldShard(localCells1, border1, WorkerId(1))
    val ws2 = createWorldShard(localCells2, border2, WorkerId(2))
    val wss = generateWorldShard(ImMap(WorkerId(1) -> localCells1, WorkerId(2) -> localCells2))
    val ws3 = wss.toSeq.head
    val ws4 = wss.toSeq.tail.head

    assert(checkEqualWorldShard(ws1, ws3))
    assert(checkEqualWorldShard(ws2, ws4))
  }

  test("test as above, but with 4 workers") {
    // Arrange
    val localCells1 = getSquare(0, 0, 2)
    val border1 = ImMap(WorkerId(2) -> ImSet(GridCellId(0, 2), GridCellId(1, 2)),
      WorkerId(3) -> ImSet(GridCellId(2, 0), GridCellId(2, 1)),
      WorkerId(4) -> ImSet(GridCellId(2, 2)))
    val localCells2 = getSquare(0, 2, 2)
    val border2 = ImMap(WorkerId(1) -> ImSet(GridCellId(0, 1), GridCellId(1, 1)),
      WorkerId(3) -> ImSet(GridCellId(2, 1)),
      WorkerId(4) -> ImSet(GridCellId(2, 2), GridCellId(2, 3)))
    val localCells3 = getSquare(2, 0, 2)
    val border3 = ImMap(WorkerId(1) -> ImSet(GridCellId(1, 0), GridCellId(1, 1)),
      WorkerId(2) -> ImSet(GridCellId(1, 2)),
      WorkerId(4) -> ImSet(GridCellId(2, 2), GridCellId(3, 2)))
    val localCells4 = getSquare(2, 2, 2)
    val border4 = ImMap(WorkerId(1) -> ImSet(GridCellId(1, 1)),
      WorkerId(2) -> ImSet(GridCellId(1, 2), GridCellId(1, 3)),
      WorkerId(3) -> ImSet(GridCellId(2, 1), GridCellId(3, 1)))
    val wss = generateWorldShard(ImMap(WorkerId(1) -> localCells1, WorkerId(2) -> localCells2, WorkerId(3) -> localCells3, WorkerId(4) -> localCells4))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get

    val ws11 = createWorldShard(localCells1, border1, WorkerId(1))
    val ws22 = createWorldShard(localCells2, border2, WorkerId(2))
    val ws33 = createWorldShard(localCells3, border3, WorkerId(3))
    val ws44 = createWorldShard(localCells4, border4, WorkerId(4))

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
  }

  test("simple change for 4 workers") {
    // Arrange
    val localCells1 = getSquare(0, 0, 2)
    val localCells2 = getSquare(0, 2, 2)
    val localCells3 = getSquare(2, 0, 2)
    val localCells4 = getSquare(2, 2, 2)
    val wss = generateWorldShard(ImMap(WorkerId(1) -> localCells1, WorkerId(2) -> localCells2, WorkerId(3) -> localCells3, WorkerId(4) -> localCells4))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    
    val balNeighbours = ImMap(WorkerId(2) -> (1, 1))
    val cellsToRemove = Set(GridCellId(0, 1), GridCellId(1, 1))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))

      val cellsToChange = balancerAlgo1.findCells(WorkerId(2), 2)
      balancerAlgo1.shrinkCells(
        cellsToChange.cells,
        cellsToChange.cellsToRemove,
        WorkerId(2),
        cellsToChange.outgoingCellsToRemove,
        cellsToChange.newIncomingCells,
        cellsToChange.newOutgoingCells,
        cellsToChange.borderOfRemainingCells)

      val (w2OutCells, w2InCells) = balancerAlgo2.expandCells(
        cellsToChange.cells,
        cellsToChange.cellsToRemove,
        WorkerId(1),
        cellsToChange.incomingCells,
        cellsToChange.outgoingCellsToRemove,
        cellsToChange.newOutgoingCells,
        cellsToChange.cellToWorker,
        cellsToChange.cellNeighbours)

    val emptyRes3 = balancerAlgo3.fixNeighbourhood(
      WorkerId(2),
      WorkerId(1),
      w2OutCells.getOrElse(WorkerId(3), Set.empty),
      cellsToChange.neighboursOutgoingCellsToRemove.getOrElse(WorkerId(3), Set.empty),
      w2InCells.getOrElse(WorkerId(3), Set.empty))
    
    val emptyRes4 = balancerAlgo4.fixNeighbourhood(
      WorkerId(2),
      WorkerId(1),
      w2OutCells.getOrElse(WorkerId(4), Set.empty),
      cellsToChange.neighboursOutgoingCellsToRemove.getOrElse(WorkerId(4), Set.empty),
      w2InCells.getOrElse(WorkerId(4), Set.empty))

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> ImSet(GridCellId(0, 0), GridCellId(1, 0)), 
      WorkerId(2) -> (getSquare(0, 2, 2) ++ cellsToRemove), 
      WorkerId(3) -> getSquare(2, 0, 2), 
      WorkerId(4) -> getSquare(2, 2, 2)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
  }
}
