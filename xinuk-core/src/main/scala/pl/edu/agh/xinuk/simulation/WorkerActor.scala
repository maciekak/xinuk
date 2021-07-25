package pl.edu.agh.xinuk.simulation

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import org.slf4j.{Logger, LoggerFactory, MarkerFactory}
import pl.edu.agh.xinuk.algorithm._
import pl.edu.agh.xinuk.balancing.{BalancerAlgo, MetricFunctions, RectangleGenerator, StatisticsData, WorldCorrectnessChecker}
import pl.edu.agh.xinuk.config.{MetricFunType, TestCaseType, XinukConfig}
import pl.edu.agh.xinuk.gui.GuiActor.GridInfo
import pl.edu.agh.xinuk.model._
import pl.edu.agh.xinuk.model.balancing.{BalancerInfo, CellsToExpand}
import pl.edu.agh.xinuk.model.grid.{GridCellId, GridWorldShard}

import scala.collection.immutable.{Map => ImMap, Set => ImSet}
import scala.collection.mutable
import scala.collection.mutable.{Map, Set}
import scala.util.Random

class WorkerActor[ConfigType <: XinukConfig](
  regionRef: => ActorRef,
  planCreator: PlanCreator[ConfigType],
  planResolver: PlanResolver[ConfigType],
  emptyMetrics: => Metrics,
  signalPropagation: SignalPropagation
)(implicit config: ConfigType) extends Actor with Stash {

  import pl.edu.agh.xinuk.simulation.WorkerActor._
  
  val BalancingInterval: Int = config.statisticsDistributionInterval * config.balancingIntervalMultiplier
  val AffectedWorkers: Seq[WorkerId] = Seq(WorkerId(2), WorkerId(5))
  val IsAreaType: Boolean = config.simulationCase == TestCaseType.AreaMiddle || config.simulationCase == TestCaseType.AreaLongRect || config.simulationCase == TestCaseType.AreaHalf || config.simulationCase == TestCaseType.AreaFreeBorders || config.simulationCase == TestCaseType.AreaSlowBorders
  val DifferentCellTime: ImMap[WorkerId, Double] = ImMap(WorkerId(1) -> 1.2, WorkerId(2) -> 1.1, WorkerId(3) -> 2, WorkerId(4) -> 4, WorkerId(5) -> 5, WorkerId(6) -> 1.4, WorkerId(7) -> 2.5, WorkerId(8) -> 0.9, WorkerId(9) -> 0.3)

  val guiActors: mutable.Set[ActorRef] = mutable.Set.empty
  val plansStash: mutable.Map[Long, Seq[Seq[TargetedPlan]]] = mutable.Map.empty.withDefaultValue(Seq.empty)
  val consequencesStash: mutable.Map[Long, Seq[Seq[TargetedStateUpdate]]] = mutable.Map.empty.withDefaultValue(Seq.empty)
  val signalUpdatesStash: mutable.Map[Long, Seq[Seq[(CellId, SignalMap)]]] = mutable.Map.empty.withDefaultValue(Seq.empty)
  val remoteCellContentsStash: mutable.Map[Long, Seq[Seq[(CellId, CellContents)]]] = mutable.Map.empty.withDefaultValue(Seq.empty)
  val statisticsStash: mutable.Map[Long, ImMap[WorkerId, StatisticsData]] = mutable.Map.empty.withDefaultValue(ImMap.empty)
  val proposeOrResignationStash: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val neighMsgFromStash: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val fixingNeighAckMsgFromStash: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val neighAckMsgFromStash: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val syncMsgFromStash: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val sync2MsgFromStash: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  
  val sendNeighMsgTo: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val receiveNeighMsgFrom: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val receiveFixingNeighAckMsgFrom: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val sendNeighAckMsgTo: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val receiveNeighAckMsgFrom: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val sendSyncMsgTo: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val receiveSyncMsgFrom: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val sendSync2MsgTo: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)
  val receiveSync2MsgFrom: mutable.Map[Long, ImSet[WorkerId]] = mutable.Map.empty.withDefaultValue(ImSet.empty)

  val savedProposal: mutable.Map[Long, Seq[Proposal]] = mutable.Map.empty.withDefaultValue(Seq.empty)
  val savedResignation: mutable.Map[Long, Seq[Resignation]] = mutable.Map.empty.withDefaultValue(Seq.empty)
  val savedUpdateNeighbourhoodEmpty: mutable.Map[Long, Seq[UpdateNeighbourhoodEmpty]] = mutable.Map.empty.withDefaultValue(Seq.empty)
  val savedUpdateNeighbourhood: mutable.Map[Long, Seq[UpdateNeighbourhood]] = mutable.Map.empty.withDefaultValue(Seq.empty)
  val savedFixNeighbourhood: mutable.Map[Long, Seq[FixNeighbourhood]] = mutable.Map.empty.withDefaultValue(Seq.empty)
  val savedAcknowledgeUpdateNeighbourhood: mutable.Map[Long, Seq[AcknowledgeUpdateNeighbourhood]] = mutable.Map.empty.withDefaultValue(Seq.empty)
  val savedSynchronizeBeforeStart: mutable.Map[Long, Seq[SynchronizeBeforeStart]] = mutable.Map.empty.withDefaultValue(Seq.empty)
  val savedSynchronizeBeforeStart2: mutable.Map[Long, Seq[SynchronizeBeforeStart2]] = mutable.Map.empty.withDefaultValue(Seq.empty)

  var logger: Logger = _
  var id: WorkerId = _
  var worldShard: WorldShard = _
  var iterationMetrics: Metrics = _
  var currentIteration: Long = _
  var phaseTime: Long = _
  var currentIterationTime: Long = _
  var blockAvgTime: Double = _
  var wholeIterationTimeStart: Long = _
  var wholeIterationTime: Long = _
  var wholeSimulationTime: Long = _
  var balancer: BalancerAlgo = _
  var toTakeCellsFrom: mutable.Map[WorkerId, (Double, Boolean)] = _
  var neutralNeigh: mutable.Map[WorkerId, (Double, Boolean)] = _
  var toSendCellsTo: mutable.Map[WorkerId, (Double, Boolean)] = _
  var waitForProposeFrom: WorkerId = _
  var sentProposeTo: WorkerId = _
  var takeCellsFrom: WorkerId = _
  var bestWorkerWhoProposed: WorkerId = _
  var sentAcceptProposal: Boolean = false
  var numberOfCellsToGive: Int = _
  var cellsToTransferReceive: CellsToExpand = _
  var balancingPhase: Int = 0 
  var defValueWorker: WorkerId = _

  override def receive: Receive = stopped

  def stopped: Receive = {

    case SubscribeGridInfo() =>
      guiActors += sender()

    case WorkerInitialized(world, balancerInfo) =>
      this.id = world.workerId
      this.worldShard = world
      this.logger = LoggerFactory.getLogger(id.value.toString)
      logger.info("starting")
      val slowAreaPlace: ImSet[CellId] = config.simulationCase match {
        case TestCaseType.AreaMiddle => RectangleGenerator.get(34, 34, 66, 66)
        case TestCaseType.AreaLongRect => RectangleGenerator.get(21, 5, 45, 95)
        case TestCaseType.AreaHalf => RectangleGenerator.get(0, 0, 50, 99)
        case TestCaseType.AreaFreeBorders => RectangleGenerator.get(15, 15, 85, 85)
        case TestCaseType.AreaSlowBorders => RectangleGenerator.get(0, 0, 99, 99) -- RectangleGenerator.get(15, 15, 85, 85)
        case _ => ImSet.empty
      }
      
      if (slowAreaPlace.nonEmpty) {
        logMetrics(0, 1, 0, 0,
          slowAreaPlace.map(g => {
            val gridCellId = g.asInstanceOf[GridCellId]
            (gridCellId.x, gridCellId.y)
          }).mkString("***_", "_", ""), iterationMetrics)
      }
      
      val metricFun: ((Int, Int), GridCellId) => Double = config.metricFunction match {
        case MetricFunType.EuclidClose => MetricFunctions.middlePointClose
        case MetricFunType.EuclidFar => MetricFunctions.middlePointFar
        case MetricFunType.MaxMin => MetricFunctions.middlePointMaxMin
      }
      
      val beginningCellsMetricsLog = if (config.shouldLogChanges) {
        val onlyCoords = worldShard.localCellIds.map(c => {
          val gridCell = c.asInstanceOf[GridCellId]
          (gridCell.x, gridCell.y)
        })
        onlyCoords.mkString(world.workerId.value.toString + "->" + this.id.value.toString + "_", "_", "")
      } else {
        ""
      }
      logMetrics(0, 0, 0, 0, beginningCellsMetricsLog, iterationMetrics)
      this.balancer = new BalancerAlgo(worldShard.asInstanceOf[GridWorldShard], 
                                       balancerInfo.workersWithMiddlePoints,
                                       metricFun,
                                       config.shouldGoDepth,
                                       config.shouldUseMetricOnAllFoundCells,
                                       config.shouldUpdateMiddlePoint,
                                       IsAreaType, 
                                       slowAreaPlace)
      wholeIterationTimeStart = System.currentTimeMillis()
      wholeSimulationTime = System.currentTimeMillis()
      planCreator.initialize(worldShard)
      if (config.shouldCheckCorrectness) {
        WorldCorrectnessChecker.initShards(world, logger, config)
      }
      self ! StartIteration(1)
      unstashAll()
      context.become(started)

    case _ =>
      stash()
  }

  def started: Receive = {

    case SubscribeGridInfo() =>
      guiActors += sender()

    case StartIteration(iteration) if iteration > config.iterationsNumber =>
      logger.info("finalizing")
      planCreator.finalize(worldShard)
      logger.info("whole simulation time: " + (System.currentTimeMillis() - wholeSimulationTime))
      logger.info("terminating")
      Thread.sleep(5000)
      context.system.terminate()

    case StartIteration(iteration) =>
      if(iteration % BalancingInterval == 1) {
        logger.info(iteration.toString + " Started iteration")
      }
      if(config.shouldCheckCorrectness && iteration % BalancingInterval == 1) {
        logMetrics(0, 0, 0, 0, iteration.toString + "\n" + "localCells:\n" + worldShard.localCellIds.toString() + "\n" + "cells:\n" + worldShard.cells.toString() + "\n" + "outgoingCells:\n" + worldShard.outgoingCells.toString() + "\n" + "incomingCells:\n" + worldShard.incomingCells.toString() + "\n" + "cellToWorker:\n" + worldShard.cellToWorker.toString() + "\n", iterationMetrics)
      }
      wholeIterationTimeStart = System.currentTimeMillis()
      phaseTime = System.currentTimeMillis()
      currentIteration = iteration
      iterationMetrics = emptyMetrics
      val plans: Seq[TargetedPlan] = worldShard.localCellIds.map(worldShard.cells(_)).flatMap(createPlans).toSeq
      if (config.simulationCase == TestCaseType.WorkerCells && AffectedWorkers.contains(this.id)){
        Thread.sleep((worldShard.localCellIds.size * config.slowMultiplier).toInt)
      }
      if (IsAreaType && balancer.slowAreaSize > 0) {
        Thread.sleep((balancer.slowAreaSize * config.slowMultiplier).toInt)
      }
      if (config.simulationCase == TestCaseType.EveryoneDifferentCellTime) {
        Thread.sleep((DifferentCellTime(this.id) * config.slowMultiplier * worldShard.localCellIds.size).toInt)
      }
      
      val timeDiff = System.currentTimeMillis() - phaseTime
      distributePlans(currentIteration, plans)
      currentIterationTime = timeDiff

    case RemotePlans(iteration, remotePlans) =>
      plansStash(iteration) :+= remotePlans
      if (plansStash(currentIteration).size == worldShard.incomingWorkerNeighbours.size) {
        phaseTime = System.currentTimeMillis()
        val shuffledPlans: Seq[TargetedPlan] = shuffleUngroup(flatGroup(plansStash(currentIteration))(_.action.target))
        val (acceptedPlans, discardedPlans) = processPlans(shuffledPlans)
        plansStash.remove(currentIteration)

        distributeConsequences(currentIteration, acceptedPlans.flatMap(_.consequence) ++ discardedPlans.flatMap(_.alternative))
        val timeDiff = System.currentTimeMillis() - phaseTime
        currentIterationTime += timeDiff
      }

    case RemoteConsequences(iteration, remoteConsequences) =>
      consequencesStash(iteration) :+= remoteConsequences
      if (consequencesStash(currentIteration).size == worldShard.incomingWorkerNeighbours.size) {
        phaseTime = System.currentTimeMillis()
        val consequences: Seq[TargetedStateUpdate] = flatGroup(consequencesStash(currentIteration))(_.target).flatMap(_._2).toSeq
        consequences.foreach(applyUpdate)
        consequencesStash.remove(currentIteration)

        val signalUpdates = calculateSignalUpdates()
        distributeSignal(currentIteration, signalUpdates)
        val timeDiff = System.currentTimeMillis() - phaseTime
        currentIterationTime += timeDiff
      }

    case RemoteSignal(iteration, remoteSignalUpdates) =>
      signalUpdatesStash(iteration) :+= remoteSignalUpdates
      if (signalUpdatesStash(currentIteration).size == worldShard.incomingWorkerNeighbours.size) {
        phaseTime = System.currentTimeMillis()
        val signalUpdates: Map[CellId, SignalMap] = flatGroup(signalUpdatesStash(currentIteration))(_._1).map {
          case (id, groups) => (id, groups.map(_._2).reduce(_ + _))
        }
        applySignalUpdates(signalUpdates)
        signalUpdatesStash.remove(currentIteration)

        distributeRemoteCellContents(currentIteration)
        val timeDiff = System.currentTimeMillis() - phaseTime
        currentIterationTime += timeDiff
      }

    case RemoteCellContents(iteration, remoteCellContents) =>
      remoteCellContentsStash(iteration) :+= remoteCellContents
      if (remoteCellContentsStash(currentIteration).size == balancer.workerCurrentNeighbours.size) {
        remoteCellContentsStash(currentIteration).flatten.foreach({
          case (cellId, cellContents) => worldShard.cells(cellId).updateContents(cellContents)
        })
        remoteCellContentsStash.remove(currentIteration)
        guiActors.foreach(_ ! GridInfo(iteration, worldShard.localCellIds.map(worldShard.cells(_)), iterationMetrics))
        if (iteration % config.statisticsDistributionInterval == 1) {
          blockAvgTime = currentIterationTime.toDouble
        } else {
          val currentDivider = if (iteration % config.statisticsDistributionInterval == 0) config.statisticsDistributionInterval
                               else iteration % config.statisticsDistributionInterval
          blockAvgTime = (blockAvgTime * (currentDivider - 1) + currentIterationTime)/currentDivider
        }
        if (iteration % 100 == 0) logger.info(s"finished $iteration")
        if (config.shouldBalance && iteration % config.statisticsDistributionInterval == 0 && iteration > 0) {
          if(config.simulationCase == TestCaseType.Mock) {
            worldShard.workerId.value match {
              case 1 => blockAvgTime = worldShard.localCellIds.size * 1.001
              case 2 => blockAvgTime = worldShard.localCellIds.size * 3
              case 3 => blockAvgTime = worldShard.localCellIds.size * 1.002
              case 4 => blockAvgTime = worldShard.localCellIds.size * 1.003
                
              case 5 => blockAvgTime = worldShard.localCellIds.size * 4
              case 6 => blockAvgTime = worldShard.localCellIds.size * 1.004
              case 7 => blockAvgTime = worldShard.localCellIds.size * 1.005
              case 8 => blockAvgTime = worldShard.localCellIds.size * 1.006
              case 9 => blockAvgTime = worldShard.localCellIds.size * 1.007
              case 10 => blockAvgTime = worldShard.localCellIds.size
              case 11 => blockAvgTime = worldShard.localCellIds.size
              case 12 => blockAvgTime = worldShard.localCellIds.size
              case 13 => blockAvgTime = worldShard.localCellIds.size
              case 14 => blockAvgTime = worldShard.localCellIds.size
              case 15 => blockAvgTime = worldShard.localCellIds.size
              case 16 => blockAvgTime = worldShard.localCellIds.size
              case _ => ()
            }
          }
          distributeStatistics(worldShard.workerId, blockAvgTime)
        } else {
          wholeIterationTime = System.currentTimeMillis() - wholeIterationTimeStart
          logMetrics(currentIteration, wholeIterationTime, currentIterationTime, blockAvgTime, "", iterationMetrics)
          self ! StartIteration(currentIteration + 1)
        }
      }

    case Statistics(iteration, neighbour, statisticsData) =>
      statisticsStash(iteration) += (neighbour -> statisticsData)
      if (statisticsStash(iteration).size == balancer.workerCurrentNeighbours.size) {
        statisticsStash(iteration).foreach(s => balancer.neighboursPlanAvgTime(s._1).addStatisticsDataBlock(s._2))
        statisticsStash.remove(iteration)

        if(iteration % BalancingInterval == 0 && iteration > 0){
          self ! StartBalancing(currentIteration)
        } else {
          self ! StartIteration(currentIteration + 1)
        }
      }

    case StartBalancing(iteration) =>
      balancingPhase = 1
      logger.info(iteration.toString + " Started balancing")
      //Czy poprawnie sortuje
      //Na pewno nie planTimeAvg
      val sortedTimes = balancer.neighboursPlanAvgTime.filter(n => worldShard.outgoingCells.keySet.contains(n._1))
        .map(n => {
        if (worldShard.outgoingCells(n._1).size + worldShard.incomingCells(n._1).size < config.minimumNumberOfAdjacentCells) {
          (0, n._2.actualBlockValue, n._1)
        } else if (n._2.actualBlockValue > blockAvgTime) {
          if (blockAvgTime / n._2.actualBlockValue <= config.minimumDiffBetweenWorkers) (1, n._2.actualBlockValue, n._1)
          else (0, n._2.actualBlockValue, n._1)
        } else {
          if (n._2.actualBlockValue / blockAvgTime <= config.minimumDiffBetweenWorkers) (-1, n._2.actualBlockValue, n._1)
          else (0, n._2.actualBlockValue, n._1)
      }
      }).toSeq
      val groups = sortedTimes.groupBy(i => i._1).map(i => i._1 -> i._2.map(ii => ii._3 -> (ii._2, false)).toMap)
      toTakeCellsFrom = mutable.Map.empty ++ groups.getOrElse(1, mutable.Map.empty)
      neutralNeigh = mutable.Map.empty ++ groups.getOrElse(0, mutable.Map.empty)
      toSendCellsTo = mutable.Map.empty ++ groups.getOrElse(-1, mutable.Map.empty)
      
      neutralNeigh.foreach(item => {
        logConditional(" -> " + item._1 + " send Resignation")
        msgDelay()
        send(regionRef, item._1, Resignation(iteration, this.id))
      })
      neutralNeigh = neutralNeigh.map(n => (n._1, (n._2._1, true)))
      waitOrSendProposal(iteration)
      savedProposal(iteration).foreach(prop => {
        logConditional(" -> " + "self" + " send Proposal")
        self ! prop
      })
      savedResignation(iteration).foreach(res => {
        logConditional(" -> " + "self" + " send Resignation")
        self ! res
      })
      savedProposal.remove(iteration)
      savedResignation.remove(iteration)
      
      //sort neighbours by time
      //for neutral send R
      //for best to take send P
      //for rest wait
    case Proposal(iteration, senderId, numberOfCells) if balancingPhase == 1 =>
      logConditional(" -> " + senderId + " receive Proposal")
      senderId match {
        case _ if waitForProposeFrom == senderId =>
          bestWorkerWhoProposed = senderId
          numberOfCellsToGive = numberOfCells
          logConditional(" -> " + bestWorkerWhoProposed + " send AcceptProposal")
          msgDelay()
          send(regionRef, bestWorkerWhoProposed, AcceptProposal(iteration, this.id))
          toSendCellsTo(senderId) = (toSendCellsTo(senderId)._1, true)
          sendResignationToRestWorkers(iteration)

        case _ if !toSendCellsTo(senderId)._2 =>
          bestWorkerWhoProposed = senderId
          numberOfCellsToGive = numberOfCells
          val bestProposalTime = toSendCellsTo(senderId)._1
          toSendCellsTo = toSendCellsTo.map(c => {
            if (!c._2._2 && bestProposalTime < c._2._1) {
              logConditional(" -> " + c._1 + " send Resignation")
              msgDelay()
              send(regionRef, c._1, Resignation(iteration, this.id))
              c._1 -> (c._2._1, true)
            } else {
              c._1 -> (c._2._1, c._2._2)
            }
          })
        case _ => ()
      }
      
      //check if better proposal wasnt propose
      //if yes, mark that got message from that worker
      //if no, add to waiting and send worse R
      //if the best, send the rest R
      //and answer with A

      proposeOrResignationStash(iteration) += senderId
      checkIfShouldGoToFixingNeighPhase(iteration)
    case Proposal(iteration, senderId, numberOfCells) =>
      logConditional(" -> " + senderId + " receive save Proposal")
      savedProposal(iteration) :+= Proposal(iteration, senderId, numberOfCells)
    case AcceptProposal(iteration, senderId) =>
      logConditional(" -> " + senderId + " receive AcceptProposal")
      takeCellsFrom = sentProposeTo
      bestWorkerWhoProposed = defValueWorker
      sentProposeTo = defValueWorker
      sendResignationToRestWorkers(iteration)
      
      proposeOrResignationStash(iteration) += senderId
      checkIfShouldGoToFixingNeighPhase(iteration)
      //send R to rest workers
      //check if response wasnt last and got to second phase
    case Resignation(iteration, senderId) if balancingPhase == 1 =>
      logConditional(" -> " + senderId + " receive Resignation")
      senderId match {
        case sid if waitForProposeFrom == senderId =>
          waitForProposeFrom = defValueWorker
          if (!toSendCellsTo(senderId)._2) {
            logConditional(" -> " + senderId + " send Resignation")
            msgDelay()
            send(regionRef, senderId, Resignation(iteration, this.id))
          }
          toSendCellsTo(senderId) = (toSendCellsTo(senderId)._1, true)
          waitOrSendProposal(iteration)
        case sid if sentProposeTo == senderId =>
          sentProposeTo = defValueWorker
          if (!toTakeCellsFrom(senderId)._2) {
            logConditional(" -> " + senderId + " send Resignation")
            msgDelay()
            send(regionRef, senderId, Resignation(iteration, this.id))
          }
          toTakeCellsFrom(senderId) = (toTakeCellsFrom(senderId)._1, true)
          waitOrSendProposal(iteration)
        case sid if toTakeCellsFrom.contains(senderId) && !toTakeCellsFrom(senderId)._2 =>
          logConditional(" -> " + senderId + " send Resignation")
          msgDelay()
          send(regionRef, senderId, Resignation(iteration, this.id))
          toTakeCellsFrom(senderId) = (toTakeCellsFrom(senderId)._1, true)
        case sid if toSendCellsTo.contains(senderId) && !toSendCellsTo(senderId)._2 =>
          logConditional(" -> " + senderId + " send Resignation")
          msgDelay()
          send(regionRef, senderId, Resignation(iteration, this.id))
          toSendCellsTo(senderId) = (toSendCellsTo(senderId)._1, true)
        case _ => ()
      }
      if (waitForProposeFrom != defValueWorker && waitForProposeFrom == bestWorkerWhoProposed) {
        logConditional(" -> " + bestWorkerWhoProposed + " send AcceptProposal")
        msgDelay()
        send(regionRef, bestWorkerWhoProposed, AcceptProposal(iteration, this.id))
        toSendCellsTo(bestWorkerWhoProposed) = (toSendCellsTo(bestWorkerWhoProposed)._1, true)
        sendResignationToRestWorkers(iteration)
      }
      proposeOrResignationStash(iteration) += senderId
      checkIfShouldGoToFixingNeighPhase(iteration)
      //If you send proposal to him then mark him as got message
      //and send proposal to next one
      //if there is no next one accept best proposal from you
      //if this message was last in this phase went to next phase
    case Resignation(iteration, senderId) =>
      logConditional(" -> " + senderId + " receive save StartUpdateNeighbourhood")
      savedResignation(iteration) :+= Resignation(iteration, senderId)
    case StartUpdateNeighbourhood(iteration) =>
      balancingPhase = 2
      logConditional("receive StartUpdateNeighbourhood")
      proposeOrResignationStash.remove(iteration)
      sendNeighMsgTo(iteration) ++= balancer.workerCurrentNeighbours
      receiveNeighMsgFrom(iteration) ++= balancer.workerCurrentNeighbours

      if(cellsToTransferReceive != null) {
        applyExpandingCells(iteration)
      } else if (bestWorkerWhoProposed != defValueWorker){
        val cellsToChange = balancer.findCells(bestWorkerWhoProposed, numberOfCellsToGive)
        
        balancer.shrinkCells(
          cellsToChange.cells,
          cellsToChange.cellsToRemove,
          cellsToChange.oldLocalCells,
          bestWorkerWhoProposed,
          cellsToChange.outgoingCellsToRemove,
          cellsToChange.newIncomingCells,
          cellsToChange.newOutgoingCells,
          cellsToChange.borderOfRemainingCells)
        val cellsToSend = new CellsToExpand(
          cellsToChange.cells,
          cellsToChange.cellsToRemove,
          this.id,
          cellsToChange.incomingCells,
          cellsToChange.outgoingCellsToRemove,
          cellsToChange.newOutgoingCells,
          cellsToChange.cellToWorker,
          cellsToChange.cellNeighbours,
          cellsToChange.neighboursOutgoingCellsToRemove)
        if(config.shouldCheckCorrectness) {
          val str = "\nworkerId:\n" + cellsToChange.workerId.toString + "\ncellsToRemove:\n" + cellsToChange.cellsToRemove.toString + "\noldLocalCells:\n" + cellsToChange.oldLocalCells.toString + "\ncells:\n" + cellsToChange.cells.toString + "\nincomingCells:\n" + cellsToChange.incomingCells.toString + "\noutgoingCells:\n" + cellsToChange.outgoingCells.toString + "\ncellToWorker:\n" + cellsToChange.cellToWorker.toString + "\ncellNeighbours:\n" + cellsToChange.cellNeighbours.toString + "\nremainingLocalCells:\n" + cellsToChange.remainingLocalCells.toString + "\nborderOfRemainingCells:\n" + cellsToChange.borderOfRemainingCells.toString + "\noutgoingCellsToRemove:\n" + cellsToChange.outgoingCellsToRemove.toString + "\nnewIncomingCells:\n" + cellsToChange.newIncomingCells.toString + "\nnewOutgoingCells:\n" + cellsToChange.newOutgoingCells.toString + "\nneighboursOutgoingCellsToRemove:\n" + cellsToChange.neighboursOutgoingCellsToRemove.toString + "\n"
          logMetrics(0, 0, 0, 0, "Cells to move: it: " + iteration.toString + "\n" + str, iterationMetrics)
        }
        logConditional(" -> " + bestWorkerWhoProposed + " send CellsTransfer")
        msgDelay()
        send(regionRef, bestWorkerWhoProposed, CellsTransfer(iteration, this.id, cellsToSend))
        distributeEmptyNeighUpdate(iteration)
      } else if (takeCellsFrom == defValueWorker) {
        distributeEmptyNeighUpdate(iteration)
      }
      
      savedUpdateNeighbourhood(iteration).foreach(un => {
        logConditional(" -> " + "self" + " send UpdateNeighbourhood")
        self ! un
      })
      savedUpdateNeighbourhoodEmpty(iteration).foreach({ une =>
        logConditional(" -> " + "self" + " send UpdateNeighbourhoodEmpty")
        self ! une 
      })
      savedFixNeighbourhood(iteration).foreach(fn => {
        logConditional(" -> " + "self" + " send FixNeighbourhood")
        self ! fn
      })
      savedUpdateNeighbourhood.remove(iteration)
      savedUpdateNeighbourhoodEmpty.remove(iteration)
      savedFixNeighbourhood.remove(iteration)
      
    case CellsTransfer(iteration, senderId, cells) =>
      logConditional(" -> " + senderId + " receive CellsTransfer in phase: " + balancingPhase)
      if(config.shouldCheckCorrectness) {
        WorldCorrectnessChecker.addChanges((senderId, this.id, cells.localCellsToChange), iteration)
      }
      cellsToTransferReceive = cells
      if (balancingPhase == 2) {
        applyExpandingCells(iteration)
      }
      //Add that cells to yourself and go to neighbour fixing phase
      
    case UpdateNeighbourhoodEmpty(iteration, senderId) if balancingPhase >= 2 =>
      logConditional(" -> " + senderId + " receive UpdateNeighbourhoodEmpty")
      neighMsgFromStash(iteration) += senderId
      if (!receiveNeighMsgFrom(iteration).contains(senderId)) {
        receiveNeighMsgFrom(iteration) += senderId
      }
      printAll(iteration, "UNe: " + balancingPhase)
      checkOrMoveToNextPhase(iteration)
      
    case UpdateNeighbourhoodEmpty(iteration, senderId) =>
      logConditional(" -> " + senderId + " receive save UpdateNeighbourhoodEmpty")
      savedUpdateNeighbourhoodEmpty(iteration) :+= UpdateNeighbourhoodEmpty(iteration, senderId)
      
    case UpdateNeighbourhood(iteration: Long,
                             newNeighbour: WorkerId,
                             oldNeighbour: WorkerId,
                             newIncomingCells: ImSet[CellId],
                             incomingCellsToRemove: ImSet[CellId],
                             newOutgoingCells: ImSet[CellId]) if balancingPhase >= 2 =>
      logConditional(" -> " + newNeighbour + " receive UpdateNeighbourhood")
      neighMsgFromStash(iteration) += newNeighbour
      val (diffNewInCells, diffInCells, diffOutCells) = balancer.fixNeighbourhood(
                                                      newNeighbour, 
                                                      oldNeighbour, 
                                                      newIncomingCells, 
                                                      incomingCellsToRemove, 
                                                      newOutgoingCells)
      
      if (bestWorkerWhoProposed != defValueWorker && (diffNewInCells.nonEmpty || diffInCells.nonEmpty || diffOutCells.nonEmpty)) {
        logConditional(" -> " + bestWorkerWhoProposed + " send FixNeighbourhood")
        msgDelay()
        send(regionRef, bestWorkerWhoProposed, FixNeighbourhood(iteration,
          this.id,
          newNeighbour,
          oldNeighbour,
          diffNewInCells,
          diffInCells,
          newOutgoingCells))
        receiveFixingNeighAckMsgFrom(iteration) += bestWorkerWhoProposed
        if (!receiveNeighMsgFrom(iteration).contains(newNeighbour)) {
          receiveNeighMsgFrom(iteration) += newNeighbour
          if (balancingPhase == 2) {
            sendNeighAckMsgTo(iteration) += newNeighbour
          }
        }
      } else if (!receiveNeighMsgFrom(iteration).contains(newNeighbour)) {
        receiveNeighMsgFrom(iteration) += newNeighbour
        if (balancingPhase == 2) {
          sendNeighAckMsgTo(iteration) += newNeighbour
        } else if (balancingPhase >= 3) {
          logConditional(" -> " + newNeighbour + " send AcknowledgeUpdateNeighbourhood")
          msgDelay()
          send(regionRef, newNeighbour, AcknowledgeUpdateNeighbourhood(iteration, this.id))
          sendNeighAckMsgTo(iteration) += newNeighbour
          receiveNeighAckMsgFrom(iteration) += newNeighbour
        }
      }
      
      checkOrMoveToNextPhase(iteration)
      //if you have new cells then send to interested workers that they have to change theirs
      //if no, then send empty messages
      //send to all formed and new neighbours
      
    case UpdateNeighbourhood(iteration: Long,
    newNeighbour: WorkerId,
    oldNeighbour: WorkerId,
    newIncomingCells: ImSet[CellId],
    incomingCellsToRemove: ImSet[CellId],
    newOutgoingCells: ImSet[CellId])  =>
      logConditional(" -> " + newNeighbour + " receive save UpdateNeighbourhood")
      savedUpdateNeighbourhood(iteration) :+= UpdateNeighbourhood(iteration, newNeighbour, oldNeighbour, newIncomingCells, incomingCellsToRemove, newOutgoingCells)
      
    case FixNeighbourhood(iteration: Long,
                          senderId: WorkerId,
                          newNeighbour: WorkerId,
                          oldNeighbour: WorkerId,
                          newIncomingCells: ImSet[CellId],
                          incomingCellsToRemove: ImSet[CellId],
                          newOutgoingCells: ImSet[CellId]) if balancingPhase >= 2 =>
      logConditional(" -> " + senderId + " receive FixNeighbourhood")
      val (diffNewIn, diffOldIn, diffOut) = balancer.fixNeighbourhood(newNeighbour, 
                                oldNeighbour, 
                                newIncomingCells, 
                                incomingCellsToRemove, 
                                newOutgoingCells)
      logConditional(" -> " + senderId + " receive AcknowledgeFixNeighbourhood")
      msgDelay()
      send(regionRef, senderId, AcknowledgeFixNeighbourhood(iteration, this.id, newNeighbour))
      if (diffNewIn.size != newIncomingCells.size || diffOldIn.size != incomingCellsToRemove.size || diffOut.size != newOutgoingCells.size) {
        if (balancingPhase == 2) {
          receiveNeighAckMsgFrom(iteration) += newNeighbour
        }
        if (balancingPhase >= 3 && !sendNeighAckMsgTo(iteration).contains(newNeighbour)) {
          receiveNeighAckMsgFrom(iteration) += newNeighbour
          logConditional(" -> " + newNeighbour + " send AcknowledgeUpdateNeighbourhood")
          msgDelay()
          send(regionRef, newNeighbour, AcknowledgeUpdateNeighbourhood(iteration, this.id))
          sendNeighAckMsgTo(iteration) += newNeighbour
          savedAcknowledgeUpdateNeighbourhood(iteration).foreach(aun => {
            logConditional(" -> " + "self" + " send AcknowledgeUpdateNeighbourhood")
            self ! aun
          })
          savedAcknowledgeUpdateNeighbourhood.remove(iteration)
        }
      }
      printAll(iteration, "FN: " + balancingPhase)
      
      //if you got UN message and you cant apply whole changes to yourself then send remaining cells to worker that took your cells
      
    case FixNeighbourhood(iteration: Long,
                          senderId: WorkerId,
                          newNeighbour: WorkerId,
                          oldNeighbour: WorkerId,
                          newIncomingCells: ImSet[CellId],
                          incomingCellsToRemove: ImSet[CellId],
                          newOutgoingCells: ImSet[CellId]) =>
      logConditional(" -> " + senderId + " receive save FixNeighbourhood")
      savedFixNeighbourhood(iteration) :+= FixNeighbourhood(iteration, senderId, newNeighbour , oldNeighbour, newIncomingCells, incomingCellsToRemove, newOutgoingCells)
      
    case AcknowledgeFixNeighbourhood(iteration, senderId, newNeighbour) =>
      logConditional(" -> " + senderId + " receive AcknowledgeFixNeighbourhood")
      fixingNeighAckMsgFromStash(iteration) += senderId
      if (balancingPhase >= 3){
        savedAcknowledgeUpdateNeighbourhood(iteration).foreach(aun => {
          logConditional(" -> " + "self" + " send AcknowledgeUpdateNeighbourhood AcknowledgeFixNeighbourhood")
          self ! aun
        })
        savedAcknowledgeUpdateNeighbourhood.remove(iteration)
        if (!sendNeighAckMsgTo(iteration).contains(newNeighbour)) {
          logConditional(" -> " + newNeighbour + " send AcknowledgeUpdateNeighbourhood")
          msgDelay()
          send(regionRef, newNeighbour, AcknowledgeUpdateNeighbourhood(iteration, this.id))
          sendNeighAckMsgTo(iteration) += newNeighbour
          receiveNeighAckMsgFrom(iteration) += newNeighbour
        }
      }
      printAll(iteration, "AFN: " + balancingPhase)
      checkOrMoveToNextPhase(iteration)
    //if you got FN then apply changes and response with AFN
      
    case AcknowledgeUpdateNeighbourhood(iteration, senderId) if balancingPhase >= 3 =>
      if (!receiveNeighAckMsgFrom(iteration).contains(senderId)){
        receiveNeighAckMsgFrom(iteration) += senderId
        logConditional(" -> " + senderId + " receive SAVE AcknowledgeUpdateNeighbourhood")
        savedAcknowledgeUpdateNeighbourhood(iteration) :+= AcknowledgeUpdateNeighbourhood(iteration, senderId)
      } else {
        logConditional(" -> " + senderId + " receive AcknowledgeUpdateNeighbourhood")
        neighAckMsgFromStash(iteration) += senderId
        if (!sendNeighAckMsgTo(iteration).contains(senderId)) {
          logConditional(" -> " + senderId + " send AcknowledgeUpdateNeighbourhood")
          msgDelay()
          send(regionRef, senderId, AcknowledgeUpdateNeighbourhood(iteration, senderId))
          sendNeighAckMsgTo(iteration) += senderId
        }
        if (balancingPhase >= 4 && !sendSyncMsgTo(iteration).contains(senderId)) {
          logConditional(" -> " + senderId + " send SynchronizeBeforeStart")
          msgDelay()
          send(regionRef, senderId, SynchronizeBeforeStart(iteration, senderId))
          sendSyncMsgTo(iteration) += senderId
          receiveSyncMsgFrom(iteration) += senderId
        }
        
        printAll(iteration, "NAck: " + balancingPhase)
        checkOrMoveToNextPhase(iteration)
      }
      
    case AcknowledgeUpdateNeighbourhood(iteration, senderId) =>
      logConditional(" -> " + senderId + " receive save AcknowledgeUpdateNeighbourhood")
      savedAcknowledgeUpdateNeighbourhood(iteration) :+= AcknowledgeUpdateNeighbourhood(iteration, senderId)
      //send if you got UN and AFN messages from workers you expected, send to all met node
      
    case SynchronizeBeforeStart(iteration, senderId) if balancingPhase >= 4 =>
      syncMsgFromStash(iteration) += senderId
      logConditional(" -> " + senderId + " receive SynchronizeBeforeStart")
      if (!receiveSyncMsgFrom(iteration).contains(senderId)) {
        receiveSyncMsgFrom(iteration) += senderId
      }
      if (!sendSyncMsgTo(iteration).contains(senderId)) {
        logConditional(" -> " + senderId + " send SynchronizeBeforeStart")
        msgDelay()
        send(regionRef, senderId, SynchronizeBeforeStart(iteration, senderId))
        sendSyncMsgTo(iteration) += senderId
      }
      if (balancingPhase == 5 && !sendSync2MsgTo(iteration).contains(senderId)) {
        logConditional(" -> " + senderId + " send SynchronizeBeforeStart2")
        msgDelay()
        send(regionRef, senderId, SynchronizeBeforeStart2(iteration, senderId))
        sendSync2MsgTo(iteration) += senderId
        receiveSync2MsgFrom(iteration) += senderId
      }
      
      printAll(iteration, "S: " + balancingPhase)
      checkOrMoveToNextPhase(iteration)
      
    case SynchronizeBeforeStart(iteration, senderId) =>
      logConditional(" -> " + senderId + " receive save SynchronizeBeforeStart")
      savedSynchronizeBeforeStart(iteration) :+= SynchronizeBeforeStart(iteration, senderId)
      
    case SynchronizeBeforeStart2(iteration, senderId) if balancingPhase == 5 =>
      sync2MsgFromStash(iteration) += senderId
      logConditional(" -> " + senderId + " receive SynchronizeBeforeStart2")
      if (!receiveSync2MsgFrom(iteration).contains(senderId)) {
        receiveSync2MsgFrom(iteration) += senderId
      }
      if (!sendSync2MsgTo(iteration).contains(senderId)) {
        logConditional(" -> " + senderId + " send SynchronizeBeforeStart2")
        msgDelay()
        send(regionRef, senderId, SynchronizeBeforeStart2(iteration, senderId))
        sendSync2MsgTo(iteration) += senderId
      }
      
      printAll(iteration, "S2: " + balancingPhase)
      checkOrMoveToNextPhase(iteration)
      
    case SynchronizeBeforeStart2(iteration, senderId) =>
      logConditional(" -> " + senderId + " receive save SynchronizeBeforeStart2")
      savedSynchronizeBeforeStart2(iteration) :+= SynchronizeBeforeStart2(iteration, senderId)
  }

  private def createPlans(cell: Cell): Seq[TargetedPlan] = {
    val neighbourStates = worldShard.cellNeighbours(cell.id)
      .map { case (direction, neighbourId) => (direction, worldShard.cells(neighbourId).state.contents) }
      .toMap
    val (plans, metrics) = planCreator.createPlans(currentIteration, cell.id, cell.state, neighbourStates)
    iterationMetrics += metrics
    plans.outwardsPlans.flatMap {
      case (direction, plans) =>
        val actionTarget = worldShard.cellNeighbours(cell.id)(direction)
        val consequenceTarget = cell.id
        val alternativeTarget = cell.id
        plans.map {
          _.toTargeted(actionTarget, consequenceTarget, alternativeTarget)
        }
    }.toSeq ++ plans.localPlans.map {
      _.toTargeted(cell.id, cell.id, cell.id)
    }
  }

  private def processPlans(plans: Seq[TargetedPlan]): (Seq[TargetedPlan], Seq[TargetedPlan]) = {
    plans.partition { plan =>
      if (validatePlan(plan)) {
        applyUpdate(plan.action)
        true
      } else {
        false
      }
    }
  }

  private def validatePlan(plan: TargetedPlan): Boolean = {
    val target = worldShard.cells(plan.action.target)
    val action = plan.action.update
    planResolver.isUpdateValid(target.state.contents, action)
  }

  private def applyUpdate(stateUpdate: TargetedStateUpdate): Unit = {
    val target = worldShard.cells(stateUpdate.target)
    val action = stateUpdate.update
    val (result, metrics) = planResolver.applyUpdate(target.state.contents, action)
    target.updateContents(result)
    iterationMetrics += metrics
  }

  private def calculateSignalUpdates(): Map[CellId, SignalMap] = {
    worldShard.calculateSignalUpdates(currentIteration, signalPropagation)
  }

  private def applySignalUpdates(signalUpdates: Map[CellId, SignalMap]): Unit = {
    signalUpdates.foreach {
      case (cellId, signalUpdate) =>
        val targetCell = worldShard.cells(cellId)
        val oldSignal = targetCell.state.signalMap
        val newSignal = (oldSignal + signalUpdate * config.signalSuppressionFactor) * config.signalAttenuationFactor
        targetCell.updateSignal(newSignal * targetCell.state.contents.signalFactor(currentIteration))
    }
  }
  
  private def printAll(iteration: Long, additionMsg: String): Unit = {
    if(config.isDebugMode){
      val text = "stash ;;; receive ;;; send - it: " + iteration + "\n" + additionMsg + "\n" + "N: " + neighMsgFromStash(iteration).toString() + ";;;" + receiveNeighMsgFrom(iteration).toString() + ";;;" + sendNeighMsgTo(iteration).toString() + "\n"        + "NAck: " + neighAckMsgFromStash(iteration).toString() + ";;;" + receiveNeighAckMsgFrom(iteration).toString() + ";;;" + sendNeighAckMsgTo(iteration).toString() + "\n"        + "FN: " + fixingNeighAckMsgFromStash(iteration).toString() + ";;;" + receiveFixingNeighAckMsgFrom(iteration).toString() + "\n"        + "S1: " + syncMsgFromStash(iteration).toString() + ";;;" + receiveSyncMsgFrom(iteration).toString() + ";;;" + sendSyncMsgTo(iteration).toString() + "\n"        + "S2: " + sync2MsgFromStash(iteration).toString() + ";;;" + receiveSync2MsgFrom(iteration).toString() + ";;;" + sendSync2MsgTo(iteration).toString() + "\n"
      logger.info(text)

      logDebugData(currentIteration, text, emptyMetrics)
    }
  }
  
  private def logConditional(info: String): Unit = {
    if (config.isDebugMode) {
      logDebugData(currentIteration, info, emptyMetrics)
      logger.info(info)
    }
  }
  
  private def waitOrSendProposal(iteration: Long): Unit = {
    if (toTakeCellsFrom.isEmpty || toTakeCellsFrom.forall(i => i._2._2)) {
      if (toSendCellsTo.nonEmpty && !toSendCellsTo.forall(i => i._2._2)) {
        waitForProposeFrom = toSendCellsTo.filter(i => !i._2._2).minBy(i => i._2._1)._1
      }
    } else if(toTakeCellsFrom.nonEmpty && toTakeCellsFrom.exists(i => !i._2._2)){
      val sendProposeTo = toTakeCellsFrom.filter(i => !i._2._2).maxBy(i => i._2._1)
      val numberOfCells = ((1.0 - blockAvgTime / sendProposeTo._2._1) / config.amountCellsDivider * worldShard.localCellIds.size).toInt
      logConditional(" -> " + sendProposeTo._1 + " send Proposal")
      msgDelay()
      send(regionRef, sendProposeTo._1, Proposal(iteration, this.id, numberOfCells))
      toTakeCellsFrom(sendProposeTo._1) = (sendProposeTo._2._1, true)
      sentProposeTo = sendProposeTo._1
    }
  }
  
  private def sendResignationToRestWorkers(iteration: Long): Unit = {
    val takeCellsKeys = toTakeCellsFrom.filter(n => !n._2._2).keys
    val sendCellsKeys = toSendCellsTo.filter(n => !n._2._2).keys
    val toSendResignation = (takeCellsKeys ++ sendCellsKeys).toSet
    toSendResignation.foreach(workerId => {
      logConditional(" -> " + workerId + " send Resignation")
      msgDelay()
      send(regionRef, workerId, Resignation(iteration, this.id))
    })
    distribute(mutable.Set.empty ++ toSendResignation, mutable.Map.empty ++ toSendResignation.map(n => n -> this.id).toMap)(this.id, id => Resignation(iteration, id))
    takeCellsKeys.foreach(key => toTakeCellsFrom(key) = (toTakeCellsFrom(key)._1, true))
    sendCellsKeys.foreach(key => toSendCellsTo(key) = (toSendCellsTo(key)._1, true))
  }
  
  private def checkOrMoveToNextPhase(iteration: Long): Unit = {
    if(balancingPhase == 2
      && receiveNeighMsgFrom(iteration).size == neighMsgFromStash(iteration).size
      && receiveFixingNeighAckMsgFrom(iteration).size == fixingNeighAckMsgFromStash(iteration).size) {
      balancingPhase = 3
      receiveNeighAckMsgFrom(iteration) ++= (receiveNeighMsgFrom(iteration) ++ receiveFixingNeighAckMsgFrom(iteration) ++ sendNeighMsgTo(iteration))
      sendNeighAckMsgTo(iteration) ++= receiveNeighAckMsgFrom(iteration)
      sendNeighAckMsgTo(iteration).foreach(workerId => {
        logConditional(" -> " + workerId + " send AcknowledgeUpdateNeighbourhood")
        msgDelay()
        send(regionRef, workerId, AcknowledgeUpdateNeighbourhood(iteration, this.id))
      })
      savedAcknowledgeUpdateNeighbourhood(iteration).foreach(aun => {
        logConditional(" -> " + "self" + " send AcknowledgeUpdateNeighbourhood")
        self ! aun
      })
      savedAcknowledgeUpdateNeighbourhood.remove(iteration)
    } 
    else if (balancingPhase == 3
      && receiveNeighAckMsgFrom(iteration).size == neighAckMsgFromStash(iteration).size
      && receiveFixingNeighAckMsgFrom(iteration).size == fixingNeighAckMsgFromStash(iteration).size) {
      balancingPhase = 4
      receiveSyncMsgFrom(iteration) ++= (receiveNeighAckMsgFrom(iteration) ++ receiveFixingNeighAckMsgFrom(iteration) ++ sendNeighAckMsgTo(iteration) ++ sendNeighAckMsgTo(iteration))
      sendSyncMsgTo(iteration) ++= receiveSyncMsgFrom(iteration)
      sendSyncMsgTo(iteration).foreach(workerId => {
        logConditional(" -> " + workerId + " send SynchronizeBeforeStart")
        msgDelay()
        send(regionRef, workerId, SynchronizeBeforeStart(iteration, this.id))
      })
      savedSynchronizeBeforeStart(iteration).foreach(sbs => {
        logConditional(" -> " + "self" + " send SynchronizeBeforeStart")
        self ! sbs
      })
      savedSynchronizeBeforeStart.remove(iteration)
    } 
    else if (balancingPhase == 4
      && receiveNeighAckMsgFrom(iteration).size == neighAckMsgFromStash(iteration).size
      && receiveFixingNeighAckMsgFrom(iteration).size == fixingNeighAckMsgFromStash(iteration).size
      && receiveSyncMsgFrom(iteration).size == syncMsgFromStash(iteration).size) {
      balancingPhase = 5
      receiveSync2MsgFrom(iteration) ++= (receiveNeighMsgFrom(iteration) ++ receiveFixingNeighAckMsgFrom(iteration) ++ receiveNeighAckMsgFrom(iteration) ++ receiveSyncMsgFrom(iteration) ++ sendSyncMsgTo(iteration))
      sendSync2MsgTo(iteration) ++= receiveSync2MsgFrom(iteration)
      sendSync2MsgTo(iteration).foreach(workerId => {
        logConditional(" -> " + workerId + " send SynchronizeBeforeStart2")
        msgDelay()
        send(regionRef, workerId, SynchronizeBeforeStart2(iteration, this.id))
      })
      savedSynchronizeBeforeStart2(iteration).foreach(sbs2 => {
        logConditional(" -> " + "self" + " send SynchronizeBeforeStart2")
        self ! sbs2
      })
      savedSynchronizeBeforeStart2.remove(iteration)
    } 
    else if (balancingPhase == 5
      && receiveNeighAckMsgFrom(iteration).size == neighAckMsgFromStash(iteration).size
      && receiveFixingNeighAckMsgFrom(iteration).size == fixingNeighAckMsgFromStash(iteration).size
      && receiveSyncMsgFrom(iteration).size == syncMsgFromStash(iteration).size
      && receiveSync2MsgFrom(iteration).size == sync2MsgFromStash(iteration).size) {
      val changeCellsMetricsLog = if (config.shouldLogChanges && cellsToTransferReceive != null) {
        val onlyCoords = cellsToTransferReceive.localCellsToChange.map(c => {
          val gridCell = c.asInstanceOf[GridCellId]
          (gridCell.x, gridCell.y)
        })
        onlyCoords.mkString(cellsToTransferReceive.workerId.value.toString + "->" + this.id.value.toString + "_", "_", "")
      } else {
        ""
      }
      
      sync2MsgFromStash.remove(iteration)
      syncMsgFromStash.remove(iteration)
      neighAckMsgFromStash.remove(iteration)
      fixingNeighAckMsgFromStash.remove(iteration)
      neighMsgFromStash.remove(iteration)
      receiveSync2MsgFrom.remove(iteration)
      receiveSyncMsgFrom.remove(iteration)
      receiveNeighAckMsgFrom.remove(iteration)
      receiveFixingNeighAckMsgFrom.remove(iteration)
      receiveNeighMsgFrom.remove(iteration)
      sendSync2MsgTo.remove(iteration)
      sendSyncMsgTo.remove(iteration)
      sendNeighAckMsgTo.remove(iteration)
      sendNeighMsgTo.remove(iteration)
      toTakeCellsFrom.clear()
      neutralNeigh.clear()
      toSendCellsTo.clear()
      waitForProposeFrom = defValueWorker
      sentProposeTo = defValueWorker
      takeCellsFrom = defValueWorker
      sentAcceptProposal = false
      numberOfCellsToGive = 0
      cellsToTransferReceive = null
      balancingPhase = 0
      wholeIterationTime = System.currentTimeMillis() - wholeIterationTimeStart
      
      logMetrics(currentIteration, wholeIterationTime, currentIterationTime, blockAvgTime, changeCellsMetricsLog, iterationMetrics)
      if (config.shouldCheckCorrectness && bestWorkerWhoProposed != defValueWorker) {
        WorldCorrectnessChecker.checkIteration(iteration)
      }
      bestWorkerWhoProposed = defValueWorker
      self ! StartIteration(iteration + 1)
    }
  }
  
  private def checkIfShouldGoToFixingNeighPhase(iteration: Long): Unit = {
    if (proposeOrResignationStash(iteration).size == worldShard.outgoingCells.keySet.size) {
      proposeOrResignationStash.remove(iteration)
      self ! StartUpdateNeighbourhood(iteration)
    }
  }
  
  private def applyExpandingCells(iteration: Long): Unit = {
    logConditional("expanding Cells from: " + cellsToTransferReceive.workerId)
    val (outCells, inCells) = balancer.expandCells(
      cellsToTransferReceive.cells,
      cellsToTransferReceive.localCellsToChange,
      cellsToTransferReceive.workerId,
      cellsToTransferReceive.incomingCells,
      cellsToTransferReceive.incomingCellsToRemove,
      cellsToTransferReceive.newIncomingCells,
      cellsToTransferReceive.cellToWorker,
      cellsToTransferReceive.cellNeighbours)
    sendNeighMsgTo(iteration) ++= worldShard.outgoingCells.keySet
    receiveNeighAckMsgFrom(iteration) ++= sendNeighMsgTo(iteration)
    sendNeighMsgTo(iteration).foreach(workerId => {
      val workerOutCells = outCells.getOrElse(workerId, ImSet.empty)
      val workerInCells = inCells.getOrElse(workerId, ImSet.empty)
      val workerOutToRemove = cellsToTransferReceive.neighboursOutgoingCellsToRemove.getOrElse(workerId, ImSet.empty)
      if (workerId == this.id || workerId == cellsToTransferReceive.workerId || workerOutCells.isEmpty && workerInCells.isEmpty && workerOutToRemove.isEmpty) {
        logConditional(" -> " + workerId + " send UpdateNeighbourhoodEmpty")
        msgDelay()
        send(regionRef, workerId, UpdateNeighbourhoodEmpty(iteration, this.id))
      } else {
        logConditional(" -> " + workerId + " send UpdateNeighbourhood")
        msgDelay()
        send(regionRef, workerId, UpdateNeighbourhood(iteration, this.id, cellsToTransferReceive.workerId, workerOutCells, workerOutToRemove, workerInCells))
      }
    })
  }
  
  private def msgDelay(): Unit = {
    if (config.balancingMessageDelay > 0) {
      Thread.sleep(config.balancingMessageDelay)
    }
  }
  
  private def distributeEmptyNeighUpdate(iteration: Long): Unit = {
    sendNeighMsgTo(iteration).foreach { workerId =>
      logConditional(" -> " + workerId + " send UpdateNeighbourhoodEmpty")
      msgDelay()
      send(regionRef, workerId, UpdateNeighbourhoodEmpty(iteration, this.id))
    }
  }

  private def distributePlans(iteration: Long, plansToDistribute: Seq[TargetedPlan]): Unit = {
    val grouped = groupByWorker(plansToDistribute) { plan => plan.action.target }
    distribute(
      worldShard.outgoingWorkerNeighbours, grouped)(
      Seq.empty, { data => RemotePlans(iteration, data) })
  }
  
  private def distributeStatistics(senderId: WorkerId, avgTime: Double): Unit = {
    distribute(
      balancer.workerCurrentNeighbours,
      balancer.workerCurrentNeighbours.zip(List.fill(balancer.workerCurrentNeighbours.size)(avgTime)).to(Map))(
      0.0, { time => 
        Statistics(currentIteration, senderId, new StatisticsData(time)) })
  }

  private def distribute[A](keys: Set[WorkerId], groups: Map[WorkerId, A])(default: => A, msgCreator: A => Any): Unit = {
    keys.foreach { workerId =>
      send(regionRef, workerId, msgCreator(groups.getOrElse(workerId, default)))
    }
  }

  private def groupByWorker[A](items: Seq[A])(idExtractor: A => CellId): Map[WorkerId, Seq[A]] = {
    items.groupBy { item => worldShard.cellToWorker(idExtractor(item)) }.to(Map)
  }

  private def distributeConsequences(iteration: Long, consequencesToDistribute: Seq[TargetedStateUpdate]): Unit = {
    val grouped = groupByWorker(consequencesToDistribute) { update => update.target }
    distribute(
      worldShard.outgoingWorkerNeighbours, grouped)(
      Seq.empty, { data => RemoteConsequences(iteration, data) })
  }

  private def distributeSignal(iteration: Long, signalToDistribute: Map[CellId, SignalMap]): Unit = {
    val grouped = groupByWorker(signalToDistribute.toSeq) { case (id, _) => id }
    distribute(
      worldShard.outgoingWorkerNeighbours, grouped)(
      Seq.empty, { data => RemoteSignal(iteration, data) })
  }

  private def distributeRemoteCellContents(iteration: Long): Unit = {
    distribute(
      worldShard.incomingWorkerNeighbours, worldShard.incomingCells)(
      Set.empty, { data => RemoteCellContents(iteration, data.toSeq.map(id => (id, worldShard.cells(id).state.contents))) })
  }

  private def logMetrics(iteration: Long, wholeTime: Long, onlyWorkerTime: Long, lastBlockTime: Double, cellsChanges: String, metrics: Metrics): Unit = {
    logger.info(WorkerActor.MetricsMarker, "{};{};{};{};{};{}", iteration.toString, wholeTime.toString, onlyWorkerTime.toString, lastBlockTime.toString, cellsChanges, metrics: Any)
  }
  
  private def logDebugData(iteration: Long, debugData: String, metrics: Metrics): Unit = {
    logger.info(WorkerActor.MetricsMarker, "{};{}------;{}", iteration.toString, debugData, metrics: Any)
  }

  private def flatGroup[A](seqs: Seq[Seq[A]])(idExtractor: A => CellId): Map[CellId, Seq[A]] = {
    seqs.flatten.groupBy {
      idExtractor(_)
    }.to(Map)
  }

  private def shuffleUngroup[*, V](groups: Map[*, Seq[V]]): Seq[V] = {
    Random.shuffle(groups.keys.toList).flatMap(k => Random.shuffle(groups(k)))
  }

//  override def receiveRecover: Receive = ???
//
//  override def receiveCommand: Receive = ???
//
//  override def persistenceId: String = "persistence-id"
}

object WorkerActor {

  final val Name: String = "WorkerActor"

  final val MetricsMarker = MarkerFactory.getMarker("METRICS")

  def props[ConfigType <: XinukConfig](regionRef: => ActorRef,
                                       planCreator: PlanCreator[ConfigType],
                                       planResolver: PlanResolver[ConfigType],
                                       emptyMetrics: => Metrics,
                                       signalPropagation: SignalPropagation)(implicit config: ConfigType): Props = {
    Props(new WorkerActor(regionRef, planCreator, planResolver, emptyMetrics, signalPropagation))
  }

  def send(ref: ActorRef, id: WorkerId, msg: Any): Unit = ref ! MsgWrapper(id, msg)

  def extractShardId(implicit config: XinukConfig): ExtractShardId = {
    case MsgWrapper(id, _) => (id.value % config.shardingMod).toString
  }

  def extractEntityId: ExtractEntityId = {
    case MsgWrapper(id, msg) =>
      (id.value.toString, msg)
  }

  final case class MsgWrapper(id: WorkerId, value: Any)

  final case class SubscribeGridInfo()

  final case class WorkerInitialized(world: WorldShard, balancerInfo: BalancerInfo)

  final case class StartIteration private(i: Long) extends AnyVal

  final case class RemotePlans private(iteration: Long, plans: Seq[TargetedPlan])

  final case class RemoteConsequences private(iteration: Long, consequences: Seq[TargetedStateUpdate])

  final case class RemoteSignal private(iteration: Long, signalUpdates: Seq[(CellId, SignalMap)])

  final case class RemoteCellContents private(iteration: Long, remoteCellContents: Seq[(CellId, CellContents)])

  final case class Statistics private(iteration: Long, senderId: WorkerId, statisticsData: StatisticsData)

  final case class StartBalancing private(iteration: Long)

  final case class Proposal private(iteration: Long, senderId: WorkerId, numberOfCells: Int)

  final case class AcceptProposal private(iteration: Long, senderId: WorkerId)

  final case class Resignation private(iteration: Long, senderId: WorkerId)

  final case class StartUpdateNeighbourhood private(iteration: Long)

  final case class CellsTransfer private(iteration: Long, senderId: WorkerId, cells: CellsToExpand)

  final case class UpdateNeighbourhoodEmpty private(iteration: Long, senderId: WorkerId)

  final case class UpdateNeighbourhood private(iteration: Long,
                                               newNeighbour: WorkerId,
                                               oldNeighbour: WorkerId,
                                               newIncomingCells: ImSet[CellId],
                                               incomingCellsToRemove: ImSet[CellId],
                                               newOutgoingCells: ImSet[CellId])

  final case class AcknowledgeUpdateNeighbourhood private(iteration: Long, senderId: WorkerId)

  final case class FixNeighbourhood private(iteration: Long,
                                            senderId: WorkerId,
                                            newNeighbour: WorkerId,
                                            oldNeighbour: WorkerId,
                                            newIncomingCells: ImSet[CellId],
                                            incomingCellsToRemove: ImSet[CellId],
                                            newOutgoingCells: ImSet[CellId])

  final case class AcknowledgeFixNeighbourhood private(iteration: Long, senderId: WorkerId, newNeighbour: WorkerId)

  final case class SynchronizeBeforeStart private(iteration: Long, senderId: WorkerId)

  final case class SynchronizeBeforeStart2 private(iteration: Long, senderId: WorkerId)

}