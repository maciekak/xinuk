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
  
  def getMetricFun(gridCells: Map[GridCellId, (Int, Int)]): ((Int, Int), GridCellId) => Double = {
    (mask: (Int, Int), cell: GridCellId) => {
      if (gridCells.contains(cell) && gridCells(cell) == mask) 1.0 else 0.0
    }
  }
  
  def getSquare(x: Int, y: Int, len: Int): ImSet[GridCellId] = {
    (for (l <- 0 until len * len) yield GridCellId(x + l % len, y + l / len)).toSet
  }
  
  def getRectangle(x1: Int, y1: Int, x2: Int, y2: Int) = {
    (for {
      x <- x1 until x2+1
      y <- y1 until y2+1
    } yield GridCellId(x, y)).toSet
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
  
  def doBalancing(balancerAlgos: Map[WorkerId, BalancerAlgo], 
                  balNeighbours: Map[WorkerId, (Int, Int)],
                  cellsToRemove: Map[GridCellId, (Int, Int)])
  : Map[WorkerId, Map[WorkerId, (Set[CellId], Set[CellId], Set[CellId])]] = {
    balNeighbours.map(worker => {
      val from = WorkerId(worker._2._1)
      val to = WorkerId(worker._2._2)
      val nonIncludedBalancers = balancerAlgos.filter(b => b._1 != from && b._1 != to)
      val cellsToChange = balancerAlgos(from).findCells(to, cellsToRemove.values.count(mask => balNeighbours(to) == mask))
      balancerAlgos(from).shrinkCells(
        cellsToChange.cells,
        cellsToChange.cellsToRemove,
        cellsToChange.oldLocalCells,
        to,
        cellsToChange.outgoingCellsToRemove,
        cellsToChange.newIncomingCells,
        cellsToChange.newOutgoingCells,
        cellsToChange.borderOfRemainingCells)
      
      val (outCells, inCells) = balancerAlgos(to).expandCells(
        cellsToChange.cells,
        cellsToChange.cellsToRemove,
        from,
        cellsToChange.incomingCells,
        cellsToChange.outgoingCellsToRemove,
        cellsToChange.newOutgoingCells,
        cellsToChange.cellToWorker,
        cellsToChange.cellNeighbours)

      val result = nonIncludedBalancers.map(b => {
        b._1 -> b._2.fixNeighbourhood(
          to,
          from,
          outCells.getOrElse(b._1, Set.empty),
          cellsToChange.neighboursOutgoingCellsToRemove.getOrElse(b._1, Set.empty),
          inCells.getOrElse(b._1, Set.empty))
      })
      
      worker._1 -> result
    })
  }
  
  def doBalancing(balancerAlgos: Map[WorkerId, BalancerAlgo],
                  from: WorkerId,
                  to: WorkerId,
                  cellsToRemove: Set[GridCellId])
  : Map[WorkerId, Map[WorkerId, (Set[CellId], Set[CellId], Set[CellId])]] = {
    doBalancing(balancerAlgos, Map(to -> (from.value, to.value)), cellsToRemove.map(c => c -> (from.value, to.value)).toMap)
  }
  
  def doBalancingFixingAtEnd(balancerAlgos: Map[WorkerId, BalancerAlgo],
                             balNeighbours: Map[WorkerId, (Int, Int)],
                             cellsToRemove: Map[GridCellId, (Int, Int)])
  : Map[WorkerId, Map[WorkerId, (Set[CellId], Set[CellId], Set[CellId], Set[CellId])]] = {
    val toFixNeighbours = balNeighbours.map(worker => {
      val from = WorkerId(worker._2._1)
      val to = WorkerId(worker._2._2)
      val nonIncludedBalancers = balancerAlgos.filter(b => b._1 != from && b._1 != to)
      val cellsToChange = balancerAlgos(from).findCells(to, cellsToRemove.values.count(mask => balNeighbours(to) == mask))
      balancerAlgos(from).shrinkCells(
        cellsToChange.cells,
        cellsToChange.cellsToRemove,
        cellsToChange.oldLocalCells,
        to,
        cellsToChange.outgoingCellsToRemove,
        cellsToChange.newIncomingCells,
        cellsToChange.newOutgoingCells,
        cellsToChange.borderOfRemainingCells)

      val (outCells, inCells) = balancerAlgos(to).expandCells(
        cellsToChange.cells,
        cellsToChange.cellsToRemove,
        from,
        cellsToChange.incomingCells,
        cellsToChange.outgoingCellsToRemove,
        cellsToChange.newOutgoingCells,
        cellsToChange.cellToWorker,
        cellsToChange.cellNeighbours)

      val result = nonIncludedBalancers.map(b => 
        b._1 -> (b._2,
          to,
          from,
          outCells.getOrElse(b._1, Set.empty),
          cellsToChange.neighboursOutgoingCellsToRemove.getOrElse(b._1, Set.empty),
          inCells.getOrElse(b._1, Set.empty)
      ))

      worker._1 -> result
    })
    
    toFixNeighbours.map(b => {
      b._1 -> b._2.map(c => c._1 -> (c._2._1.fixNeighbourhood(c._2._2, c._2._3, c._2._4, c._2._5, c._2._6), c._2._6))
    }).map(i => i._1 -> i._2.map(inn => inn._1 -> (inn._2._1._1, inn._2._1._2, inn._2._1._3, inn._2._2)))
  }

  def doBalancingFixingAtEnd(balancerAlgos: Map[WorkerId, BalancerAlgo],
                  from: WorkerId,
                  to: WorkerId,
                  cellsToRemove: Set[GridCellId])
  : Map[WorkerId, Map[WorkerId, (Set[CellId], Set[CellId], Set[CellId], Set[CellId])]] = {
    doBalancingFixingAtEnd(balancerAlgos, Map(to -> (from.value, to.value)), cellsToRemove.map(c => c -> (from.value, to.value)).toMap)
  }
  
  def doFixingNeighbours(balancerAlgos: Map[WorkerId, BalancerAlgo],
                         balNeighbours: Map[WorkerId, (Int, Int)],
                         unhandledCells: Map[WorkerId, Map[WorkerId, (Set[CellId], Set[CellId], Set[CellId], Set[CellId])]]) = {
    unhandledCells.map(u => u._1 -> u._2.filter(i => i._2._1.nonEmpty || i._2._2.nonEmpty || i._2._3.nonEmpty))
      .foreach(u => {
        u._2.foreach(c => {
          val oldNeighVal = balNeighbours(u._1)._1
          val to = balNeighbours.find(b => b._2._1 == c._1.value).get._1
          balancerAlgos(to).fixNeighbourhood(u._1, WorkerId(oldNeighVal), c._2._1, c._2._2, c._2._4)
        })
      })
    
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
        cellsToChange.oldLocalCells,
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

  test("Case number 1") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getSquare(0, 0, 2), 
        WorkerId(2) -> getSquare(0, 3, 2), 
        WorkerId(3) -> (getRectangle(2, 0, 3, 4) ++ getRectangle(0, 2, 1, 2))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get

    val balNeighbours = ImMap(WorkerId(1) -> (1, 1))
    val cellsToRemove = Set(GridCellId(0, 2), GridCellId(1, 2))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))

    doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3), 
      WorkerId(3), 
      WorkerId(1), 
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 0, 1, 2),
      WorkerId(2) -> getSquare(0, 3, 2),
      WorkerId(3) -> getRectangle(2, 0, 3, 4)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
  }

  test("Case number 2") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getSquare(0, 0, 2),
      WorkerId(2) -> getRectangle(0, 2, 3, 2),
      WorkerId(3) -> getSquare(0, 3, 2),
      WorkerId(4) -> getSquare(2, 0, 2),
      WorkerId(5) -> getSquare(2, 3, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get

    val balNeighbours = ImMap(WorkerId(1) -> (1, 1))
    val cellsToRemove = Set(GridCellId(0, 2), GridCellId(1, 2))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4, WorkerId(5) -> balancerAlgo5),
      WorkerId(2),
      WorkerId(1),
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 0, 1, 2),
      WorkerId(2) -> getRectangle(2, 2, 3, 2),
      WorkerId(3) -> getSquare(0, 3, 2),
      WorkerId(4) -> getSquare(2, 0, 2),
      WorkerId(5) -> getSquare(2, 3, 2)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
  }

  test("Case number 3 - in same time") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getSquare(0, 0, 2),
      WorkerId(2) -> getRectangle(0, 2, 3, 2),
      WorkerId(3) -> getSquare(0, 3, 2),
      WorkerId(4) -> getSquare(2, 0, 2),
      WorkerId(5) -> getSquare(2, 3, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get

    val balNeighbours = ImMap(WorkerId(1) -> (2, 1), WorkerId(4) -> (2, 4))
    val cellsToRemove = Map(GridCellId(1, 2) -> (2, 1), GridCellId(2, 2) -> (2, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4, WorkerId(5) -> balancerAlgo5),
      balNeighbours,
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getSquare(0, 0, 2) + GridCellId(1, 2)),
      WorkerId(2) -> Set(GridCellId(0, 2), GridCellId(3, 2)),
      WorkerId(3) -> getSquare(0, 3, 2),
      WorkerId(4) -> (getSquare(2, 0, 2) + GridCellId(2, 2)),
      WorkerId(5) -> getSquare(2, 3, 2)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
  }

  test("Case number 4a - one column of cells") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(1, 0, 2, 4),
      WorkerId(2) -> (getRectangle(0, 0, 0, 4) ++ getRectangle(3, 0, 4, 4))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get

    val balNeighbours = ImMap(WorkerId(2) -> (1, 1))
    val cellsToRemove = Set(GridCellId(1, 2), GridCellId(2, 2))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2),
      WorkerId(1),
      WorkerId(2),
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getSquare(1, 3, 2) ++ getSquare(1, 0, 2)),
      WorkerId(2) -> (getRectangle(0, 0, 0, 4) ++ getRectangle(3, 0, 4, 4) ++ getRectangle(1, 2, 2, 2))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
  }

  test("Case number 4b - two column of cells") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(1, 0, 2, 5),
      WorkerId(2) -> (getRectangle(0, 0, 0, 5) ++ getRectangle(3, 0, 4, 5))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get

    val balNeighbours = ImMap(WorkerId(2) -> (1, 1))
    val cellsToRemove = Set(GridCellId(1, 2), GridCellId(2, 2), GridCellId(1, 3), GridCellId(2, 3))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2),
      WorkerId(1),
      WorkerId(2),
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getSquare(1, 4, 2) ++ getSquare(1, 0, 2)),
      WorkerId(2) -> (getRectangle(0, 0, 0, 5) ++ getRectangle(3, 0, 4, 5) ++ getRectangle(1, 2, 2, 3))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
  }

  test("Case number 4c - three column of cells") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(1, 0, 2, 6),
      WorkerId(2) -> (getRectangle(0, 0, 0, 6) ++ getRectangle(3, 0, 4, 6))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get

    val balNeighbours = ImMap(WorkerId(2) -> (1, 1))
    val cellsToRemove = getRectangle(1, 2, 2, 4)
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2),
      WorkerId(1),
      WorkerId(2),
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getSquare(1, 5, 2) ++ getSquare(1, 0, 2)),
      WorkerId(2) -> (getRectangle(0, 0, 0, 6) ++ getRectangle(3, 0, 4, 6) ++ getRectangle(1, 2, 2, 4))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
  }

  test("Case number 5") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 0, 3) ++ getRectangle(3, 0, 3, 3)),
      WorkerId(2) -> getRectangle(1, 0, 2, 3)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get

    val balNeighbours = ImMap(WorkerId(2) -> (1, 1))
    val cellsToRemove = getRectangle(0, 0, 0, 3)
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2),
      WorkerId(1),
      WorkerId(2),
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(3, 0, 3, 3),
      WorkerId(2) -> getRectangle(0, 0, 2, 3)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
  }

  test("Case number 6") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getSquare(0, 0, 2),
      WorkerId(2) -> (getRectangle(2, 0, 2, 2) ++ getRectangle(0, 2, 1, 2)),
      WorkerId(3) -> getRectangle(0, 3, 1, 3),
      WorkerId(4) -> getRectangle(2, 3, 2, 3)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get

    val balNeighbours = ImMap(WorkerId(1) -> (1, 1))
    val cellsToRemove = getRectangle(0, 2, 1, 2)
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4),
      WorkerId(2),
      WorkerId(1),
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getSquare(0, 0, 2) ++ getRectangle(0, 2, 1, 2)),
      WorkerId(2) -> getRectangle(2, 0, 2, 2),
      WorkerId(3) -> getRectangle(0, 3, 1, 3),
      WorkerId(4) -> getRectangle(2, 3, 2, 3)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
  }

  test("Case number 7") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 0, 1) ++ getRectangle(1, 1, 1, 1)),
      WorkerId(2) -> (getRectangle(1, 0, 1, 0) ++ getRectangle(0, 2, 0, 2)),
      WorkerId(3) -> getRectangle(1, 2, 1, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get

    val balNeighbours = ImMap(WorkerId(3) -> (1, 1))
    val cellsToRemove = getRectangle(0, 2, 0, 2)
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3),
      WorkerId(2),
      WorkerId(3),
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 0, 1) ++ getRectangle(1, 1, 1, 1)),
      WorkerId(2) -> getRectangle(1, 0, 1, 0),
      WorkerId(3) -> (getRectangle(1, 2, 1, 2) ++ getRectangle(0, 2, 0, 2))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
  }

  test("Case number 8a - one by one") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getSquare(0, 0, 2),
      WorkerId(2) -> getSquare(2, 0, 2),
      WorkerId(3) -> getSquare(2, 2, 2),
      WorkerId(4) -> getSquare(0, 2, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get

    val balNeighbours = ImMap(WorkerId(1) -> (2, 1), WorkerId(2) -> (3, 2), WorkerId(3) -> (4, 3), WorkerId(4) -> (1, 4))
    val cellsToRemove = Map(GridCellId(2, 1) -> (2, 1), GridCellId(2, 2) -> (3, 2), GridCellId(1, 2) -> (4, 3), GridCellId(1, 1) -> (1, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4),
      balNeighbours,
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getSquare(0, 0, 2) - GridCellId(1, 1) + GridCellId(2, 1)),
      WorkerId(2) -> (getSquare(2, 0, 2) - GridCellId(2, 1) + GridCellId(2, 2)),
      WorkerId(3) -> (getSquare(2, 2, 2) - GridCellId(2, 2) + GridCellId(1, 2)),
      WorkerId(4) -> (getSquare(0, 2, 2) - GridCellId(1, 2) + GridCellId(1, 1))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
  }

  test("Case number 8b - in same time - not passed because of giving and getting cells in same time") {
    //TODO: maybe in the future
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getSquare(0, 0, 2),
      WorkerId(2) -> getSquare(2, 0, 2),
      WorkerId(3) -> getSquare(2, 2, 2),
      WorkerId(4) -> getSquare(0, 2, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get

    val balNeighbours = ImMap(WorkerId(1) -> (2, 1), WorkerId(2) -> (3, 2), WorkerId(3) -> (4, 3), WorkerId(4) -> (1, 4))
    val cellsToRemove = Map(GridCellId(2, 1) -> (2, 1), GridCellId(2, 2) -> (3, 2), GridCellId(1, 2) -> (4, 3), GridCellId(1, 1) -> (1, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))

    val balancerAlgos = Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4)
    val unhandledCells = doBalancingFixingAtEnd(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)
    doFixingNeighbours(balancerAlgos, balNeighbours, unhandledCells)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getSquare(0, 0, 2) - GridCellId(1, 1) + GridCellId(2, 1)),
      WorkerId(2) -> (getSquare(2, 0, 2) - GridCellId(2, 1) + GridCellId(2, 2)),
      WorkerId(3) -> (getSquare(2, 2, 2) - GridCellId(2, 2) + GridCellId(1, 2)),
      WorkerId(4) -> (getSquare(0, 2, 2) - GridCellId(1, 2) + GridCellId(1, 1))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
  }

  test("Case number 9a - one by one") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 1, 2) ++ getRectangle(2, 0, 2, 1)),
      WorkerId(2) -> (getRectangle(4, 0, 5, 2) ++ getRectangle(3, 0, 3, 1)),
      WorkerId(3) -> (getRectangle(4, 3, 5, 5) ++ getRectangle(3, 4, 3, 5)),
      WorkerId(4) -> (getRectangle(0, 3, 1, 5) ++ getRectangle(2, 4, 2, 5)),
      WorkerId(5) -> getSquare(2, 2, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get

    val balNeighbours = ImMap(WorkerId(1) -> (2, 1), WorkerId(2) -> (3, 2), WorkerId(3) -> (4, 3), WorkerId(4) -> (1, 4))
    val cellsToRemove = Map(GridCellId(3, 1) -> (2, 1), GridCellId(4, 3) -> (3, 2), GridCellId(2, 4) -> (4, 3), GridCellId(1, 2) -> (1, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4, WorkerId(5) -> balancerAlgo5),
      balNeighbours,
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 1, 2) ++ getRectangle(2, 0, 2, 1) - GridCellId(1, 2) + GridCellId(3, 1)),
      WorkerId(2) -> (getRectangle(4, 0, 5, 2) ++ getRectangle(3, 0, 3, 1) - GridCellId(3, 1) + GridCellId(4, 3)),
      WorkerId(3) -> (getRectangle(4, 3, 5, 5) ++ getRectangle(3, 4, 3, 5) - GridCellId(4, 3) + GridCellId(2, 4)),
      WorkerId(4) -> (getRectangle(0, 3, 1, 5) ++ getRectangle(2, 4, 2, 5) - GridCellId(2, 4) + GridCellId(1, 2)),
      WorkerId(5) -> getSquare(2, 2, 2)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
  }

  test("Case number 9b - in same time") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 1, 2) ++ getRectangle(2, 0, 2, 1)),
      WorkerId(2) -> (getRectangle(4, 0, 5, 2) ++ getRectangle(3, 0, 3, 1)),
      WorkerId(3) -> (getRectangle(4, 3, 5, 5) ++ getRectangle(3, 4, 3, 5)),
      WorkerId(4) -> (getRectangle(0, 3, 1, 5) ++ getRectangle(2, 4, 2, 5)),
      WorkerId(5) -> getSquare(2, 2, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get

    val balNeighbours = ImMap(WorkerId(1) -> (2, 1), WorkerId(2) -> (3, 2), WorkerId(3) -> (4, 3), WorkerId(4) -> (1, 4))
    val cellsToRemove = Map(GridCellId(3, 1) -> (2, 1), GridCellId(4, 3) -> (3, 2), GridCellId(2, 4) -> (4, 3), GridCellId(1, 2) -> (1, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancingFixingAtEnd(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4, WorkerId(5) -> balancerAlgo5),
      balNeighbours,
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 1, 2) ++ getRectangle(2, 0, 2, 1) - GridCellId(1, 2) + GridCellId(3, 1)),
      WorkerId(2) -> (getRectangle(4, 0, 5, 2) ++ getRectangle(3, 0, 3, 1) - GridCellId(3, 1) + GridCellId(4, 3)),
      WorkerId(3) -> (getRectangle(4, 3, 5, 5) ++ getRectangle(3, 4, 3, 5) - GridCellId(4, 3) + GridCellId(2, 4)),
      WorkerId(4) -> (getRectangle(0, 3, 1, 5) ++ getRectangle(2, 4, 2, 5) - GridCellId(2, 4) + GridCellId(1, 2)),
      WorkerId(5) -> getSquare(2, 2, 2)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
  }

  test("Case number 10") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 0, 1, 0),
      WorkerId(2) -> getRectangle(0, 1, 1, 1),
      WorkerId(3) -> getRectangle(0, 2, 1, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get

    val balNeighbours = ImMap(WorkerId(1) -> (1, 1))
    val cellsToRemove = getRectangle(0, 1, 1, 1)
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3),
      WorkerId(2),
      WorkerId(1),
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 0, 1, 1),
      WorkerId(3) -> getRectangle(0, 2, 1, 2)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws3, ws33))
  }

  test("Case number 11") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> Set(GridCellId(0, 0), GridCellId(0, 2)),
      WorkerId(2) -> (getRectangle(1, 0, 1, 2) + GridCellId(0, 1))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get

    val balNeighbours = ImMap(WorkerId(1) -> (1, 1))
    val cellsToRemove = Set(GridCellId(0, 1))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2),
      WorkerId(2),
      WorkerId(1),
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 0, 0, 2),
      WorkerId(2) -> getRectangle(1, 0, 1, 2)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
  }

  test("Case number 12a") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 1, 0) + GridCellId(1, 1)),
      WorkerId(2) -> getSquare(0, 2, 2),
      WorkerId(3) -> getSquare(2, 0, 2),
      WorkerId(4) -> Set(GridCellId(0, 1)),
      WorkerId(5) -> Set(GridCellId(3, 2)),
      WorkerId(6) -> (getRectangle(2, 3, 3, 3) + GridCellId(2, 2))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get
    val ws6 = wss.find(w => w.workerId == WorkerId(6)).get

    val balNeighbours = ImMap(WorkerId(4) -> (1, 4), WorkerId(5) -> (6, 5))
    val cellsToRemove = Map(GridCellId(1, 1) -> (1, 4), GridCellId(2, 2) -> (6, 5))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo6 = new BalancerAlgo(ws6, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4, WorkerId(5) -> balancerAlgo5, WorkerId(6) -> balancerAlgo6),
      balNeighbours,
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 0, 1, 0),
      WorkerId(2) -> getSquare(0, 2, 2),
      WorkerId(3) -> getSquare(2, 0, 2),
      WorkerId(4) -> getRectangle(0, 1, 1, 1),
      WorkerId(5) -> getRectangle(2, 2, 3, 2),
      WorkerId(6) -> getRectangle(2, 3, 3, 3)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get
    val ws66 = wss2.find(w => w.workerId == WorkerId(6)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
    assert(checkEqualWorldShard(ws6, ws66))
  }

  test("Case number 12b") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 1, 0) + GridCellId(1, 1)),
      WorkerId(2) -> getSquare(0, 2, 2),
      WorkerId(3) -> getSquare(2, 0, 2),
      WorkerId(4) -> Set(GridCellId(0, 1)),
      WorkerId(5) -> Set(GridCellId(3, 2)),
      WorkerId(6) -> (getRectangle(2, 3, 3, 3) + GridCellId(2, 2))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get
    val ws6 = wss.find(w => w.workerId == WorkerId(6)).get

    val balNeighbours = ImMap(WorkerId(4) -> (1, 4), WorkerId(5) -> (6, 5))
    val cellsToRemove = Map(GridCellId(1, 1) -> (1, 4), GridCellId(2, 2) -> (6, 5))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo6 = new BalancerAlgo(ws6, balNeighbours, getMetricFun(cellsToRemove))

    val balancerAlgos = Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4, WorkerId(5) -> balancerAlgo5, WorkerId(6) -> balancerAlgo6)
    val unhandledCells = doBalancingFixingAtEnd(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)
    doFixingNeighbours(balancerAlgos, balNeighbours, unhandledCells)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 0, 1, 0),
      WorkerId(2) -> getSquare(0, 2, 2),
      WorkerId(3) -> getSquare(2, 0, 2),
      WorkerId(4) -> getRectangle(0, 1, 1, 1),
      WorkerId(5) -> getRectangle(2, 2, 3, 2),
      WorkerId(6) -> getRectangle(2, 3, 3, 3)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get
    val ws66 = wss2.find(w => w.workerId == WorkerId(6)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
    assert(checkEqualWorldShard(ws6, ws66))
  }

  test("Case number 13a") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 1, 1, 2) + GridCellId(1, 3)),
      WorkerId(2) -> Set(GridCellId(0, 3)),
      WorkerId(3) -> getSquare(0, 4, 2),
      WorkerId(4) -> (getRectangle(3, 1, 3, 4) + GridCellId(2, 1)),
      WorkerId(5) -> (getRectangle(0, 0, 4, 0) ++ getRectangle(4, 1, 4, 5) ++ getRectangle(2, 5, 3, 5) ++ getRectangle(2, 3, 2, 4)),
      WorkerId(6) -> Set(GridCellId(2, 2))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get
    val ws6 = wss.find(w => w.workerId == WorkerId(6)).get

    val balNeighbours = ImMap(WorkerId(2) -> (1, 2), WorkerId(4) -> (5, 4))
    val cellsToRemove = Map(GridCellId(1, 3) -> (1, 2), GridCellId(2, 3) -> (5, 4), GridCellId(2, 4) -> (5, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo6 = new BalancerAlgo(ws6, balNeighbours, getMetricFun(cellsToRemove))

    val unhandledCells = doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4, WorkerId(5) -> balancerAlgo5, WorkerId(6) -> balancerAlgo6),
      balNeighbours,
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 1, 1, 2),
      WorkerId(2) -> getRectangle(0, 3, 1, 3),
      WorkerId(3) -> getSquare(0, 4, 2),
      WorkerId(4) -> (getRectangle(3, 1, 3, 4) ++ getRectangle(2, 3, 2, 4) + GridCellId(2, 1)),
      WorkerId(5) -> (getRectangle(0, 0, 4, 0) ++ getRectangle(4, 1, 4, 5) ++ getRectangle(2, 5, 3, 5)),
      WorkerId(6) -> Set(GridCellId(2, 2))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get
    val ws66 = wss2.find(w => w.workerId == WorkerId(6)).get

    unhandledCells.foreach(u => u._2.foreach(c => {
      assert(c._2._1.isEmpty)
      assert(c._2._2.isEmpty)
      assert(c._2._3.isEmpty)
    }))
    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
    assert(checkEqualWorldShard(ws6, ws66))
  }

  test("Case number 13b") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 1, 1, 2) + GridCellId(1, 3)),
      WorkerId(2) -> Set(GridCellId(0, 3)),
      WorkerId(3) -> getSquare(0, 4, 2),
      WorkerId(4) -> (getRectangle(3, 1, 3, 4) + GridCellId(2, 1)),
      WorkerId(5) -> (getRectangle(0, 0, 4, 0) ++ getRectangle(4, 1, 4, 5) ++ getRectangle(2, 5, 3, 5) ++ getRectangle(2, 3, 2, 4)),
      WorkerId(6) -> Set(GridCellId(2, 2))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get
    val ws6 = wss.find(w => w.workerId == WorkerId(6)).get

    val balNeighbours = ImMap(WorkerId(2) -> (1, 2), WorkerId(4) -> (5, 4))
    val cellsToRemove = Map(GridCellId(1, 3) -> (1, 2), GridCellId(2, 3) -> (5, 4), GridCellId(2, 4) -> (5, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo6 = new BalancerAlgo(ws6, balNeighbours, getMetricFun(cellsToRemove))

    val balancerAlgos = Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4, WorkerId(5) -> balancerAlgo5, WorkerId(6) -> balancerAlgo6)
    val unhandledCells = doBalancingFixingAtEnd(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)
    doFixingNeighbours(balancerAlgos, balNeighbours, unhandledCells)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 1, 1, 2),
      WorkerId(2) -> getRectangle(0, 3, 1, 3),
      WorkerId(3) -> getSquare(0, 4, 2),
      WorkerId(4) -> (getRectangle(3, 1, 3, 4) ++ getRectangle(2, 3, 2, 4) + GridCellId(2, 1)),
      WorkerId(5) -> (getRectangle(0, 0, 4, 0) ++ getRectangle(4, 1, 4, 5) ++ getRectangle(2, 5, 3, 5)),
      WorkerId(6) -> Set(GridCellId(2, 2))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get
    val ws66 = wss2.find(w => w.workerId == WorkerId(6)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
    assert(checkEqualWorldShard(ws6, ws66))
  }

  test("Case number 14a") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getSquare(0, 0, 2),
      WorkerId(2) -> getSquare(2, 0, 2),
      WorkerId(3) -> getSquare(2, 2, 2),
      WorkerId(4) -> getSquare(0, 2, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get

    val balNeighbours = ImMap(WorkerId(1) -> (2, 1), WorkerId(2) -> (3, 2), WorkerId(3) -> (4, 3))
    val cellsToRemove = Map(GridCellId(2, 1) -> (2, 1), GridCellId(2, 2) -> (3, 2), GridCellId(1, 2) -> (4, 3))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))

    val balancerAlgos = Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4)
    val unhandledCells = doBalancing(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getSquare(0, 0, 2) + GridCellId(2, 1)),
      WorkerId(2) -> (getSquare(2, 0, 2) - GridCellId(2, 1) + GridCellId(2, 2)),
      WorkerId(3) -> (getSquare(2, 2, 2) - GridCellId(2, 2) + GridCellId(1, 2)),
      WorkerId(4) -> (getSquare(0, 2, 2) - GridCellId(1, 2))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
  }

  test("Case number 14b - in same time - not passed because of giving and getting cells in same time") {
    //TODO: maybe in the future
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getSquare(0, 0, 2),
      WorkerId(2) -> getSquare(2, 0, 2),
      WorkerId(3) -> getSquare(2, 2, 2),
      WorkerId(4) -> getSquare(0, 2, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get

    val balNeighbours = ImMap(WorkerId(1) -> (2, 1), WorkerId(2) -> (3, 2), WorkerId(3) -> (4, 3))
    val cellsToRemove = Map(GridCellId(2, 1) -> (2, 1), GridCellId(2, 2) -> (3, 2), GridCellId(1, 2) -> (4, 3))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))

    val balancerAlgos = Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2, WorkerId(3) -> balancerAlgo3, WorkerId(4) -> balancerAlgo4)
    val unhandledCells = doBalancingFixingAtEnd(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)
    doFixingNeighbours(balancerAlgos, balNeighbours, unhandledCells)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getSquare(0, 0, 2) + GridCellId(2, 1)),
      WorkerId(2) -> (getSquare(2, 0, 2) - GridCellId(2, 1) + GridCellId(2, 2)),
      WorkerId(3) -> (getSquare(2, 2, 2) - GridCellId(2, 2) + GridCellId(1, 2)),
      WorkerId(4) -> (getSquare(0, 2, 2) - GridCellId(1, 2))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
  }

  test("Case number 15 - in same time") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> Set(GridCellId(0, 2), GridCellId(0, 3), GridCellId(7, 2), GridCellId(7, 3)),
      WorkerId(2) -> getSquare(0, 0, 2),
      WorkerId(3) -> getSquare(1, 2, 2),
      WorkerId(4) -> getSquare(2, 0, 2),
      WorkerId(5) -> getSquare(3, 2, 2),
      WorkerId(6) -> getSquare(4, 0, 2),
      WorkerId(7) -> getSquare(5, 2, 2),
      WorkerId(8) -> getSquare(6, 0, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get
    val ws6 = wss.find(w => w.workerId == WorkerId(6)).get
    val ws7 = wss.find(w => w.workerId == WorkerId(7)).get
    val ws8 = wss.find(w => w.workerId == WorkerId(8)).get

    val balNeighbours = ImMap(
      WorkerId(1) -> (7, 1),
      WorkerId(2) -> (4, 2),
      WorkerId(5) -> (3, 5),
      WorkerId(6) -> (8, 6))
    val cellsToRemove = Map(
      GridCellId(6, 2) -> (7, 1),
      GridCellId(2, 1) -> (4, 2),
      GridCellId(2, 2) -> (3, 5),
      GridCellId(6, 1) -> (8, 6))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo6 = new BalancerAlgo(ws6, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo7 = new BalancerAlgo(ws7, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo8 = new BalancerAlgo(ws8, balNeighbours, getMetricFun(cellsToRemove))

    val balancerAlgos = Map(
      WorkerId(1) -> balancerAlgo1,
      WorkerId(2) -> balancerAlgo2,
      WorkerId(3) -> balancerAlgo3,
      WorkerId(4) -> balancerAlgo4,
      WorkerId(5) -> balancerAlgo5,
      WorkerId(6) -> balancerAlgo6,
      WorkerId(7) -> balancerAlgo7,
      WorkerId(8) -> balancerAlgo8)
    val unhandledCells = doBalancingFixingAtEnd(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)
    doFixingNeighbours(balancerAlgos, balNeighbours, unhandledCells)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> Set(GridCellId(0, 2), GridCellId(0, 3), GridCellId(7, 2), GridCellId(7, 3), GridCellId(6, 2)),
      WorkerId(2) -> (getSquare(0, 0, 2) + GridCellId(2, 1)),
      WorkerId(3) -> (getSquare(1, 2, 2) - GridCellId(2, 2)),
      WorkerId(4) -> (getSquare(2, 0, 2) - GridCellId(2, 1)),
      WorkerId(5) -> (getSquare(3, 2, 2) + GridCellId(2, 2)),
      WorkerId(6) -> (getSquare(4, 0, 2) + GridCellId(6, 1)),
      WorkerId(7) -> (getSquare(5, 2, 2) - GridCellId(6, 2)),
      WorkerId(8) -> (getSquare(6, 0, 2) - GridCellId(6, 1))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get
    val ws66 = wss2.find(w => w.workerId == WorkerId(6)).get
    val ws77 = wss2.find(w => w.workerId == WorkerId(7)).get
    val ws88 = wss2.find(w => w.workerId == WorkerId(8)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
    assert(checkEqualWorldShard(ws6, ws66))
    assert(checkEqualWorldShard(ws7, ws77))
    assert(checkEqualWorldShard(ws8, ws88))
  }

  test("Case number 16 - in same time") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 4, 0) + GridCellId(2, 2)),
      WorkerId(2) -> Set(GridCellId(2, 1), GridCellId(1, 1), GridCellId(1, 2)),
      WorkerId(3) -> (getRectangle(0, 1, 0, 5) + GridCellId(3, 2)),
      WorkerId(4) -> Set(GridCellId(1, 3), GridCellId(1, 4), GridCellId(2, 4)),
      WorkerId(5) -> (getRectangle(5, 0, 5, 4) + GridCellId(3, 3)),
      WorkerId(6) -> Set(GridCellId(3, 4), GridCellId(4, 4), GridCellId(4, 3)),
      WorkerId(7) -> (getRectangle(1, 5, 5, 5) + GridCellId(2, 3)),
      WorkerId(8) -> Set(GridCellId(3, 1), GridCellId(4, 1), GridCellId(4, 2))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get
    val ws6 = wss.find(w => w.workerId == WorkerId(6)).get
    val ws7 = wss.find(w => w.workerId == WorkerId(7)).get
    val ws8 = wss.find(w => w.workerId == WorkerId(8)).get

    val balNeighbours = ImMap(
      WorkerId(2) -> (3, 2),
      WorkerId(8) -> (5, 8),
      WorkerId(6) -> (7, 6),
      WorkerId(4) -> (1, 4))
    val cellsToRemove = Map(
      GridCellId(3, 2) -> (3, 2),
      GridCellId(3, 3) -> (5, 8),
      GridCellId(2, 3) -> (7, 6),
      GridCellId(2, 2) -> (1, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo6 = new BalancerAlgo(ws6, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo7 = new BalancerAlgo(ws7, balNeighbours, getMetricFun(cellsToRemove))
    val balancerAlgo8 = new BalancerAlgo(ws8, balNeighbours, getMetricFun(cellsToRemove))

    val balancerAlgos = Map(
      WorkerId(1) -> balancerAlgo1,
      WorkerId(2) -> balancerAlgo2,
      WorkerId(3) -> balancerAlgo3,
      WorkerId(4) -> balancerAlgo4,
      WorkerId(5) -> balancerAlgo5,
      WorkerId(6) -> balancerAlgo6,
      WorkerId(7) -> balancerAlgo7,
      WorkerId(8) -> balancerAlgo8)
    val unhandledCells = doBalancingFixingAtEnd(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)
    doFixingNeighbours(balancerAlgos, balNeighbours, unhandledCells)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 0, 4, 0),
      WorkerId(2) -> Set(GridCellId(2, 1), GridCellId(1, 1), GridCellId(1, 2), GridCellId(3, 2)),
      WorkerId(3) -> getRectangle(0, 1, 0, 5),
      WorkerId(4) -> Set(GridCellId(1, 3), GridCellId(1, 4), GridCellId(2, 4), GridCellId(2, 2)),
      WorkerId(5) -> getRectangle(5, 0, 5, 4),
      WorkerId(6) -> Set(GridCellId(3, 4), GridCellId(4, 4), GridCellId(4, 3), GridCellId(2, 3)),
      WorkerId(7) -> getRectangle(1, 5, 5, 5),
      WorkerId(8) -> Set(GridCellId(3, 1), GridCellId(4, 1), GridCellId(4, 2), GridCellId(3, 3))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get
    val ws66 = wss2.find(w => w.workerId == WorkerId(6)).get
    val ws77 = wss2.find(w => w.workerId == WorkerId(7)).get
    val ws88 = wss2.find(w => w.workerId == WorkerId(8)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
    assert(checkEqualWorldShard(ws6, ws66))
    assert(checkEqualWorldShard(ws7, ws77))
    assert(checkEqualWorldShard(ws8, ws88))
  }

  /***************** MULTIPLE CELLS, NOT ONLY BORDER *****************************/
  test("Case number 17 ") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 0, 2, 0),
      WorkerId(2) -> getSquare(0, 1, 3)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get

    val balNeighbours = ImMap(WorkerId(1) -> (1, 1))
    val cellsToRemove = getRectangle(0, 1, 2, 2) + GridCellId(1, 3)
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove), true)

    doBalancing(
      Map(WorkerId(1) -> balancerAlgo1, WorkerId(2) -> balancerAlgo2),
      WorkerId(2),
      WorkerId(1),
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 2, 2) + GridCellId(1, 3)),
      WorkerId(2) -> Set(GridCellId(0, 3), GridCellId(2, 3))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
  }

  test("Case number 18a") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> Set(GridCellId(0, 0), GridCellId(1, 0)),
      WorkerId(2) -> getRectangle(0, 1, 1, 3),
      WorkerId(3) -> getRectangle(0, 4, 1, 6),
      WorkerId(4) -> getSquare(0, 7, 2),
      WorkerId(5) -> (getRectangle(2, 0, 3, 8) -- Set(GridCellId(2, 3), GridCellId(2, 4), GridCellId(2, 5))),
      WorkerId(6) -> Set(GridCellId(2, 3), GridCellId(2, 4), GridCellId(2, 5))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get
    val ws6 = wss.find(w => w.workerId == WorkerId(6)).get

    val balNeighbours = ImMap(
      WorkerId(1) -> (2, 1),
      WorkerId(4) -> (3, 4))
    val cellsToRemove = Map(
      GridCellId(0, 1) -> (2, 1),
      GridCellId(0, 2) -> (2, 1),
      GridCellId(1, 1) -> (2, 1),
      GridCellId(1, 2) -> (2, 1),
      GridCellId(1, 3) -> (2, 1),
      GridCellId(0, 6) -> (3, 4),
      GridCellId(0, 5) -> (3, 4),
      GridCellId(0, 4) -> (3, 4),
      GridCellId(1, 5) -> (3, 4),
      GridCellId(1, 6) -> (3, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo6 = new BalancerAlgo(ws6, balNeighbours, getMetricFun(cellsToRemove), true)

    val balancerAlgos = Map(
      WorkerId(1) -> balancerAlgo1,
      WorkerId(2) -> balancerAlgo2,
      WorkerId(3) -> balancerAlgo3,
      WorkerId(4) -> balancerAlgo4,
      WorkerId(5) -> balancerAlgo5,
      WorkerId(6) -> balancerAlgo6)

    val unhandledCells = doBalancing(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 1, 3) - GridCellId(0, 3)),
      WorkerId(2) -> Set(GridCellId(0, 3)),
      WorkerId(3) -> Set(GridCellId(1, 4)),
      WorkerId(4) -> (getRectangle(0, 4, 1, 8) - GridCellId(1, 4)),
      WorkerId(5) -> (getRectangle(2, 0, 3, 8) -- Set(GridCellId(2, 3), GridCellId(2, 4), GridCellId(2, 5))),
      WorkerId(6) -> Set(GridCellId(2, 3), GridCellId(2, 4), GridCellId(2, 5))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get
    val ws66 = wss2.find(w => w.workerId == WorkerId(6)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
    assert(checkEqualWorldShard(ws6, ws66))
  }

  test("Case number 18b") {
    // Arrange
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> Set(GridCellId(0, 0), GridCellId(1, 0)),
      WorkerId(2) -> getRectangle(0, 1, 1, 3),
      WorkerId(3) -> getRectangle(0, 4, 1, 6),
      WorkerId(4) -> getSquare(0, 7, 2),
      WorkerId(5) -> (getRectangle(2, 0, 3, 8) -- Set(GridCellId(2, 3), GridCellId(2, 4), GridCellId(2, 5))),
      WorkerId(6) -> Set(GridCellId(2, 3), GridCellId(2, 4), GridCellId(2, 5))))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get
    val ws5 = wss.find(w => w.workerId == WorkerId(5)).get
    val ws6 = wss.find(w => w.workerId == WorkerId(6)).get

    val balNeighbours = ImMap(
      WorkerId(1) -> (2, 1),
      WorkerId(4) -> (3, 4))
    val cellsToRemove = Map(
      GridCellId(0, 1) -> (2, 1),
      GridCellId(0, 2) -> (2, 1),
      GridCellId(1, 1) -> (2, 1),
      GridCellId(1, 2) -> (2, 1),
      GridCellId(1, 3) -> (2, 1),
      GridCellId(0, 6) -> (3, 4),
      GridCellId(0, 5) -> (3, 4),
      GridCellId(0, 4) -> (3, 4),
      GridCellId(1, 5) -> (3, 4),
      GridCellId(1, 6) -> (3, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo5 = new BalancerAlgo(ws5, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo6 = new BalancerAlgo(ws6, balNeighbours, getMetricFun(cellsToRemove), true)

    val balancerAlgos = Map(
      WorkerId(1) -> balancerAlgo1, 
      WorkerId(2) -> balancerAlgo2, 
      WorkerId(3) -> balancerAlgo3, 
      WorkerId(4) -> balancerAlgo4, 
      WorkerId(5) -> balancerAlgo5, 
      WorkerId(6) -> balancerAlgo6)
      
    val unhandledCells = doBalancingFixingAtEnd(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)
    doFixingNeighbours(balancerAlgos, balNeighbours, unhandledCells)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 1, 3) - GridCellId(0, 3)),
      WorkerId(2) -> Set(GridCellId(0, 3)),
      WorkerId(3) -> Set(GridCellId(1, 4)),
      WorkerId(4) -> (getRectangle(0, 4, 1, 8) - GridCellId(1, 4)),
      WorkerId(5) -> (getRectangle(2, 0, 3, 8) -- Set(GridCellId(2, 3), GridCellId(2, 4), GridCellId(2, 5))),
      WorkerId(6) -> Set(GridCellId(2, 3), GridCellId(2, 4), GridCellId(2, 5))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get
    val ws55 = wss2.find(w => w.workerId == WorkerId(5)).get
    val ws66 = wss2.find(w => w.workerId == WorkerId(6)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
    assert(checkEqualWorldShard(ws5, ws55))
    assert(checkEqualWorldShard(ws6, ws66))
  }

  test("Case number 19") {
    // Arrange
    //TODO: nie przechodzi naprawić
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getSquare(0, 0, 2),
      WorkerId(2) -> getRectangle(2, 0, 4, 1),
      WorkerId(3) -> getRectangle(0, 2, 4, 4),
      WorkerId(4) -> getRectangle(0, 5, 4, 6)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get

    val balNeighbours = ImMap(
      WorkerId(1) -> (2, 1),
      WorkerId(4) -> (3, 4))
    val cellsToRemove = Map(
      GridCellId(2, 0) -> (2, 1),
      GridCellId(2, 1) -> (2, 1),
      GridCellId(3, 0) -> (2, 1),
      GridCellId(3, 1) -> (2, 1),
      GridCellId(4, 1) -> (2, 1),
      GridCellId(4, 2) -> (3, 4),
      GridCellId(4, 3) -> (3, 4),
      GridCellId(4, 4) -> (3, 4),
      GridCellId(3, 3) -> (3, 4),
      GridCellId(3, 4) -> (3, 4),
      GridCellId(2, 3) -> (3, 4),
      GridCellId(2, 4) -> (3, 4),
      GridCellId(1, 3) -> (3, 4),
      GridCellId(1, 4) -> (3, 4),
      GridCellId(0, 3) -> (3, 4),
      GridCellId(0, 4) -> (3, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove), true)

    val balancerAlgos = Map(
      WorkerId(1) -> balancerAlgo1,
      WorkerId(2) -> balancerAlgo2,
      WorkerId(3) -> balancerAlgo3,
      WorkerId(4) -> balancerAlgo4)

    val unhandledCells = doBalancingFixingAtEnd(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)
    doFixingNeighbours(balancerAlgos, balNeighbours, unhandledCells)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> (getRectangle(0, 0, 4, 1) - GridCellId(4, 0)),
      WorkerId(2) -> Set(GridCellId(4, 0)),
      WorkerId(3) -> getRectangle(0, 2, 2, 2),
      WorkerId(4) -> (getRectangle(0, 2, 4, 6) -- getRectangle(0, 2, 2, 2))))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
  }

  test("Case number 20a") {
    // Arrange
    //TODO: nie przechodzi naprawić
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getSquare(0, 0, 2),
      WorkerId(2) -> (getSquare(0, 2, 2) + GridCellId(3, 3)),
      WorkerId(3) -> (getSquare(0, 4, 2) + GridCellId(5, 5)),
      WorkerId(4) -> getSquare(0, 6, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get

    val balNeighbours = ImMap(
      WorkerId(1) -> (2, 1),
      WorkerId(4) -> (3, 4))
    val cellsToRemove = Map(
      GridCellId(0, 2) -> (2, 1),
      GridCellId(0, 3) -> (2, 1),
      GridCellId(1, 2) -> (2, 1),
      GridCellId(1, 3) -> (2, 1),
      GridCellId(0, 4) -> (3, 4),
      GridCellId(0, 5) -> (3, 4),
      GridCellId(1, 4) -> (3, 4),
      GridCellId(1, 5) -> (3, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove), true)

    val balancerAlgos = Map(
      WorkerId(1) -> balancerAlgo1,
      WorkerId(2) -> balancerAlgo2,
      WorkerId(3) -> balancerAlgo3,
      WorkerId(4) -> balancerAlgo4)

    val unhandledCells = doBalancingFixingAtEnd(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)
    doFixingNeighbours(balancerAlgos, balNeighbours, unhandledCells)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 0, 3, 1),
      WorkerId(2) -> Set(GridCellId(3, 3)),
      WorkerId(3) -> Set(GridCellId(5, 5)),
      WorkerId(4) -> getRectangle(0, 4, 1, 7)))
    val ws11 = wss2.find(w => w.workerId == WorkerId(1)).get
    val ws22 = wss2.find(w => w.workerId == WorkerId(2)).get
    val ws33 = wss2.find(w => w.workerId == WorkerId(3)).get
    val ws44 = wss2.find(w => w.workerId == WorkerId(4)).get

    assert(checkEqualWorldShard(ws1, ws11))
    assert(checkEqualWorldShard(ws2, ws22))
    assert(checkEqualWorldShard(ws3, ws33))
    assert(checkEqualWorldShard(ws4, ws44))
  }

  test("Case number 20b") {
    // Arrange
    //TODO: nie przechodzi naprawić
    val wss = generateWorldShard(ImMap(
      WorkerId(1) -> getSquare(0, 0, 2),
      WorkerId(2) -> (getSquare(0, 2, 2) + GridCellId(3, 3)),
      WorkerId(3) -> (getSquare(0, 4, 2) + GridCellId(3, 4)),
      WorkerId(4) -> getSquare(0, 6, 2)))
    val ws1 = wss.find(w => w.workerId == WorkerId(1)).get
    val ws2 = wss.find(w => w.workerId == WorkerId(2)).get
    val ws3 = wss.find(w => w.workerId == WorkerId(3)).get
    val ws4 = wss.find(w => w.workerId == WorkerId(4)).get

    val balNeighbours = ImMap(
      WorkerId(1) -> (2, 1),
      WorkerId(4) -> (3, 4))
    val cellsToRemove = Map(
      GridCellId(0, 2) -> (2, 1),
      GridCellId(0, 3) -> (2, 1),
      GridCellId(1, 2) -> (2, 1),
      GridCellId(1, 3) -> (2, 1),
      GridCellId(0, 4) -> (3, 4),
      GridCellId(0, 5) -> (3, 4),
      GridCellId(1, 4) -> (3, 4),
      GridCellId(1, 5) -> (3, 4))
    val balancerAlgo1 = new BalancerAlgo(ws1, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo2 = new BalancerAlgo(ws2, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo3 = new BalancerAlgo(ws3, balNeighbours, getMetricFun(cellsToRemove), true)
    val balancerAlgo4 = new BalancerAlgo(ws4, balNeighbours, getMetricFun(cellsToRemove), true)

    val balancerAlgos = Map(
      WorkerId(1) -> balancerAlgo1,
      WorkerId(2) -> balancerAlgo2,
      WorkerId(3) -> balancerAlgo3,
      WorkerId(4) -> balancerAlgo4)

    val unhandledCells = doBalancingFixingAtEnd(
      balancerAlgos,
      balNeighbours,
      cellsToRemove)
    doFixingNeighbours(balancerAlgos, balNeighbours, unhandledCells)

    val wss2 = generateWorldShard(ImMap(
      WorkerId(1) -> getRectangle(0, 0, 3, 1),
      WorkerId(2) -> Set(GridCellId(3, 3)),
      WorkerId(3) -> Set(GridCellId(3, 4)),
      WorkerId(4) -> getRectangle(0, 4, 1, 7)))
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
