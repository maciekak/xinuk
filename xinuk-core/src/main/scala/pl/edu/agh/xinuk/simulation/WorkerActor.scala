package pl.edu.agh.xinuk.simulation

import akka.actor.{Actor, ActorRef, Props, Stash}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import org.slf4j.{Logger, LoggerFactory, MarkerFactory}
import pl.edu.agh.xinuk.algorithm._
import pl.edu.agh.xinuk.balancing.{BalancerAlgo, MetricFunctions, StatisticsData, WorldCorrectnessChecker}
import pl.edu.agh.xinuk.config.XinukConfig
import pl.edu.agh.xinuk.gui.GuiActor.GridInfo
import pl.edu.agh.xinuk.model._
import pl.edu.agh.xinuk.model.balancing.{BalancerInfo, CellsToExpand}
import pl.edu.agh.xinuk.model.grid.GridWorldShard

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
  
  val StatisticsDistributionInterval = 10
  val BalancingIntervalMultiplier = 1
  val BalancingInterval: Int = StatisticsDistributionInterval * BalancingIntervalMultiplier
  val ShouldGoDepth: Boolean = true

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
  var consequenceTimeAvg: Double = _
  var balancer: BalancerAlgo = _
  var toTakeCellsFrom: mutable.Map[WorkerId, (Double, Boolean)] = _
  var neutralNeigh: mutable.Map[WorkerId, (Double, Boolean)] = _
  var toSendCellsTo: mutable.Map[WorkerId, (Double, Boolean)] = _
  var waitForProposeFrom: WorkerId = _
  var sentProposeTo: WorkerId = _
  var takeCellsFrom: WorkerId = _
  var bestWorkerWhoProposed: WorkerId = _
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
      this.balancer = new BalancerAlgo(worldShard.asInstanceOf[GridWorldShard], balancerInfo.workersWithMiddlePoints, MetricFunctions.middlePointFar, ShouldGoDepth)
      planCreator.initialize(worldShard)
      WorldCorrectnessChecker.initShards(world, logger, config)
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
      logger.info("terminating")
      Thread.sleep(5000)
      context.system.terminate()

    case StartIteration(iteration) =>
      phaseTime = System.currentTimeMillis()
      if(this.id == WorkerId(1) && iteration == 250) {
        Thread.sleep(100)
      }
      currentIteration = iteration
      iterationMetrics = emptyMetrics
      val plans: Seq[TargetedPlan] = worldShard.localCellIds.map(worldShard.cells(_)).flatMap(createPlans).toSeq
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

        logMetrics(currentIteration, iterationMetrics)
        guiActors.foreach(_ ! GridInfo(iteration, worldShard.localCellIds.map(worldShard.cells(_)), iterationMetrics))
        if (iteration == 0) {
          blockAvgTime = currentIterationTime.toDouble
        } else {
          blockAvgTime = (blockAvgTime * (iteration-1) + currentIterationTime)/iteration
        }
        logger.info(iteration.toString + " RemotePlans: " + blockAvgTime)
        if(iteration == 20 && worldShard.workerId == WorkerId(1)){
          logger.info("dupa")
        }
        if (iteration % 100 == 0) logger.info(s"finished $iteration")
        if (iteration % StatisticsDistributionInterval == 0 && iteration > 0) {
          worldShard.workerId.value match {
            case 1 => blockAvgTime = 500.0
            case 2 => blockAvgTime = 350.0
            case 3 => blockAvgTime = 300.0
            case 4 => blockAvgTime = 350.0
            case _ => ()
          }
          distributeStatistics(worldShard.workerId, blockAvgTime)
        } else {
          self ! StartIteration(currentIteration + 1)
        }
      }

    case Statistics(iteration, neighbour, statisticsData) =>
      statisticsStash(iteration) += (neighbour -> statisticsData)
      if(iteration > 10){
        logger.info("Receive statistcs from: " + neighbour + "\n" + statisticsStash(iteration).toString())
      }
      if (statisticsStash(iteration).size == balancer.workerCurrentNeighbours.size) {
        statisticsStash(iteration).foreach(
          s => balancer.neighboursPlanAvgTime(s._1).addStatisticsDataBlock(s._2))
        
        statisticsStash.remove(iteration)

        if(iteration == 20 && (worldShard.workerId == WorkerId(1) || worldShard.workerId == WorkerId(4))){
          logger.info("DUPA: ")
        }
        if(iteration % BalancingInterval == 0 && iteration > 0){
          self ! StartBalancing(currentIteration)
        } else {
          self ! StartIteration(currentIteration + 1)
        }
      }

    case StartBalancing(iteration) =>
      if(iteration == 20 && this.id == WorkerId(4)) {
        logger.info("dupa")
      }
      balancingPhase = 1
      //Czy poprawnie sortuje
      //Na pewno nie planTimeAvg
      val sortedTimes = balancer.neighboursPlanAvgTime.filter(n => worldShard.outgoingCells.keySet.contains(n._1))
        .map(n => {
        val minimumNumberOfAdjacentCells = 4
        if (worldShard.outgoingCells(n._1).size + worldShard.incomingCells(n._1).size < minimumNumberOfAdjacentCells) {
          (0, n._2.actualBlockValue, n._1)
        } else if (n._2.actualBlockValue > blockAvgTime) {
          if (blockAvgTime / n._2.actualBlockValue <= 0.85) (1, n._2.actualBlockValue, n._1)
          else (0, n._2.actualBlockValue, n._1)
        } else {
          if (n._2.actualBlockValue / blockAvgTime <= 0.85) (-1, n._2.actualBlockValue, n._1)
          else (0, n._2.actualBlockValue, n._1)
      }
      }).toSeq.sortBy(i => i._2)
      val groups = sortedTimes.groupBy(i => i._1).map(i => i._1 -> i._2.map(ii => ii._3 -> (ii._2, false)).toMap)
      toTakeCellsFrom = mutable.Map.empty ++ groups.getOrElse(1, mutable.Map.empty)
      neutralNeigh = mutable.Map.empty ++ groups.getOrElse(0, mutable.Map.empty)
      toSendCellsTo = mutable.Map.empty ++ groups.getOrElse(-1, mutable.Map.empty)
      
      neutralNeigh.foreach(item => {
        send(regionRef, item._1, Resignation(iteration, this.id))
      })
      neutralNeigh = neutralNeigh.map(n => (n._1, (n._2._1, true)))
      waitOrSendProposal(iteration)
      savedProposal(iteration).foreach(prop => self ! prop)
      savedResignation(iteration).foreach(res => self ! res)
      savedProposal.remove(iteration)
      savedResignation.remove(iteration)
      
      //sort neighbours by time
      //for neutral send R
      //for best to take send P
      //for rest wait
    case Proposal(iteration, senderId, numberOfCells) if balancingPhase == 1 =>
      senderId match {
        case _ if waitForProposeFrom == senderId =>
          bestWorkerWhoProposed = senderId
          numberOfCellsToGive = numberOfCells
          send(regionRef, bestWorkerWhoProposed, AcceptProposal(iteration, this.id))
          toSendCellsTo(senderId) = (toSendCellsTo(senderId)._1, true)
          sendResignationToRestWorkers(iteration)

        case _ if !toSendCellsTo(senderId)._2 =>
          bestWorkerWhoProposed = senderId
          numberOfCellsToGive = numberOfCells
          var startMarking = false
          toSendCellsTo = toSendCellsTo.map(c => {
            if (startMarking) {
              if (!c._2._2) {
                send(regionRef, c._1, Resignation(iteration, this.id))
              }
              c._1 -> (c._2._1, true)
            } else {
              if (c._1 == senderId) {
                startMarking = true
              }
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
      savedProposal(iteration) :+= Proposal(iteration, senderId, numberOfCells)
    case AcceptProposal(iteration, senderId) => 
      takeCellsFrom = sentProposeTo
      sentProposeTo = defValueWorker
      sendResignationToRestWorkers(iteration)
      
      proposeOrResignationStash(iteration) += senderId
      checkIfShouldGoToFixingNeighPhase(iteration)
      //send R to rest workers
      //check if response wasnt last and got to second phase
    case Resignation(iteration, senderId) if balancingPhase == 1 =>
      senderId match {
        case sid if waitForProposeFrom == senderId =>
          waitForProposeFrom = defValueWorker
          if (!toSendCellsTo(senderId)._2) {
            send(regionRef, senderId, Resignation(iteration, this.id))
          }
          toSendCellsTo(senderId) = (toSendCellsTo(senderId)._1, true)
          waitOrSendProposal(iteration)
          if (waitForProposeFrom != defValueWorker && waitForProposeFrom == bestWorkerWhoProposed) {
            send(regionRef, bestWorkerWhoProposed, AcceptProposal(iteration, this.id))
            toSendCellsTo(bestWorkerWhoProposed) = (toSendCellsTo(bestWorkerWhoProposed)._1, true)
            sendResignationToRestWorkers(iteration)
          }
        case sid if sentProposeTo == senderId =>
          sentProposeTo = defValueWorker
          if (!toTakeCellsFrom(senderId)._2) {
            send(regionRef, senderId, Resignation(iteration, this.id))
          }
          toTakeCellsFrom(senderId) = (toTakeCellsFrom(senderId)._1, true)
          waitOrSendProposal(iteration)
        case sid if toTakeCellsFrom.contains(senderId) && !toTakeCellsFrom(senderId)._2 =>
          send(regionRef, senderId, Resignation(iteration, this.id))
          toTakeCellsFrom(senderId) = (toTakeCellsFrom(senderId)._1, true)
        case sid if toSendCellsTo.contains(senderId) && !toSendCellsTo(senderId)._2 =>
          send(regionRef, senderId, Resignation(iteration, this.id))
          toSendCellsTo(senderId) = (toSendCellsTo(senderId)._1, true)
        case _ => ()
      }
      proposeOrResignationStash(iteration) += senderId
      checkIfShouldGoToFixingNeighPhase(iteration)
      //If you send proposal to him then mark him as got message
      //and send proposal to next one
      //if there is no next one accept best proposal from you
      //if this message was last in this phase went to next phase
    case Resignation(iteration, senderId) =>
      savedResignation(iteration) :+= Resignation(iteration, senderId)
    case StartUpdateNeighbourhood(iteration) =>
      balancingPhase = 2
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
        send(regionRef, bestWorkerWhoProposed, CellsTransfer(iteration, this.id, cellsToSend))
        distributeEmptyNeighUpdate(iteration)
      } else if (takeCellsFrom == defValueWorker) {
        distributeEmptyNeighUpdate(iteration)
      }
      
      savedUpdateNeighbourhood(iteration).foreach(un => self ! un)
      savedUpdateNeighbourhoodEmpty(iteration).foreach(une => self ! une)
      savedFixNeighbourhood(iteration).foreach(fn => self ! fn)
      savedUpdateNeighbourhood.remove(iteration)
      savedUpdateNeighbourhoodEmpty.remove(iteration)
      savedFixNeighbourhood.remove(iteration)
      
    case CellsTransfer(iteration, senderId, cells) =>
      WorldCorrectnessChecker.addChanges((senderId, this.id, cells.localCellsToChange), iteration)
      cellsToTransferReceive = cells
      if (balancingPhase == 2) {
        applyExpandingCells(iteration)
      }
      //Add that cells to yourself and go to neighbour fixing phase
      
    case UpdateNeighbourhoodEmpty(iteration, senderId) if balancingPhase >= 2 =>
      neighMsgFromStash(iteration) += senderId
      if (!sendNeighAckMsgTo(iteration).contains(senderId)) {
        sendNeighAckMsgTo(iteration) += senderId
        if (balancingPhase > 2) {
          send(regionRef, senderId, AcknowledgeUpdateNeighbourhood(iteration, this.id))
        }
      }
      if (!receiveNeighMsgFrom(iteration).contains(senderId)) {
        addSenderToFutureReceiver(iteration, senderId)
        receiveNeighMsgFrom(iteration) += senderId
      }
      printAll(iteration, "UNe: " + balancingPhase)
      if (balancingPhase == 2
        && receiveNeighMsgFrom(iteration).size == neighMsgFromStash(iteration).size
        && receiveFixingNeighAckMsgFrom(iteration).size == fixingNeighAckMsgFromStash(iteration).size) {
        balancingPhase = 3
        sendNeighAckMsgTo(iteration).foreach(workerId => {
          send(regionRef, workerId, AcknowledgeUpdateNeighbourhood(iteration, this.id))
        })
        receiveSyncMsgFrom(iteration) ++= sendNeighAckMsgTo(iteration)
        sendSyncMsgTo(iteration) ++= sendNeighAckMsgTo(iteration)
      }
      checkOrMoveToNextPhase(iteration)
      
    case UpdateNeighbourhoodEmpty(iteration, senderId) if balancingPhase < 2 =>
      savedUpdateNeighbourhoodEmpty(iteration) :+= UpdateNeighbourhoodEmpty(iteration, senderId)
      
    case UpdateNeighbourhood(iteration: Long,
                             newNeighbour: WorkerId,
                             oldNeighbour: WorkerId,
                             newIncomingCells: ImSet[CellId],
                             incomingCellsToRemove: ImSet[CellId],
                             newOutgoingCells: ImSet[CellId]) if balancingPhase >= 2 =>
      neighMsgFromStash(iteration) += newNeighbour
      if (!sendNeighAckMsgTo(iteration).contains(newNeighbour)) {
        sendNeighAckMsgTo(iteration) += newNeighbour
        if(balancingPhase > 2) {
          send(regionRef, newNeighbour, AcknowledgeUpdateNeighbourhood(iteration, this.id))
        }
      }
      if (!receiveNeighMsgFrom(iteration).contains(newNeighbour)) {
        addSenderToFutureReceiver(iteration, newNeighbour)
        receiveNeighMsgFrom(iteration) += newNeighbour
      }
      val (diffNewInCells, diffInCells, diffOutCells) = balancer.fixNeighbourhood(
                                                      newNeighbour, 
                                                      oldNeighbour, 
                                                      newIncomingCells, 
                                                      incomingCellsToRemove, 
                                                      newOutgoingCells)
      
      if (bestWorkerWhoProposed != defValueWorker && (diffNewInCells.nonEmpty || diffInCells.nonEmpty || diffOutCells.nonEmpty)) {
        send(regionRef, bestWorkerWhoProposed, FixNeighbourhood(iteration,
          this.id,
          newNeighbour,
          oldNeighbour,
          diffNewInCells,
          diffInCells,
          newOutgoingCells))
        receiveFixingNeighAckMsgFrom(iteration) += bestWorkerWhoProposed
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
    newOutgoingCells: ImSet[CellId]) if balancingPhase < 2 =>
      savedUpdateNeighbourhood(iteration) :+= UpdateNeighbourhood(iteration, newNeighbour, oldNeighbour, newIncomingCells, incomingCellsToRemove, newOutgoingCells)
      
    case FixNeighbourhood(iteration: Long,
                          senderId: WorkerId,
                          newNeighbour: WorkerId,
                          oldNeighbour: WorkerId,
                          newIncomingCells: ImSet[CellId],
                          incomingCellsToRemove: ImSet[CellId],
                          newOutgoingCells: ImSet[CellId]) if balancingPhase >= 2 =>
      val (diffNewIn, diffOldIn, diffOut) = balancer.fixNeighbourhood(newNeighbour, 
                                oldNeighbour, 
                                newIncomingCells, 
                                incomingCellsToRemove, 
                                newOutgoingCells)
      send(regionRef, senderId, AcknowledgeFixNeighbourhood(iteration, this.id))
      printAll(iteration, "FN: " + balancingPhase)
      if (diffNewIn.size != newIncomingCells.size || diffOldIn.size != incomingCellsToRemove.size || diffOut.size != newOutgoingCells.size) {
        if (balancingPhase == 2) {
          receiveNeighAckMsgFrom(iteration) += newNeighbour
        }
        if (balancingPhase >= 3 && !sendNeighAckMsgTo(iteration).contains(newNeighbour)) {
          send(regionRef, newNeighbour, AcknowledgeUpdateNeighbourhood(iteration, this.id))
        }
        sendNeighAckMsgTo(iteration) += newNeighbour
        addSenderToFutureReceiver(iteration, newNeighbour)
      }
      
      //if you got UN message and you cant apply whole changes to yourself then send remaining cells to worker that took your cells
      
    case FixNeighbourhood(iteration: Long,
                          senderId: WorkerId,
                          newNeighbour: WorkerId,
                          oldNeighbour: WorkerId,
                          newIncomingCells: ImSet[CellId],
                          incomingCellsToRemove: ImSet[CellId],
                          newOutgoingCells: ImSet[CellId]) =>
      savedFixNeighbourhood(iteration) :+= FixNeighbourhood(iteration, senderId, newNeighbour , oldNeighbour, newIncomingCells, incomingCellsToRemove, newOutgoingCells)
      
    case AcknowledgeFixNeighbourhood(iteration, senderId) =>
      fixingNeighAckMsgFromStash(iteration) += senderId
      printAll(iteration, "AFN: " + balancingPhase)
      checkOrMoveToNextPhase(iteration)
    //if you got FN then apply changes and response with AFN
      
    case AcknowledgeUpdateNeighbourhood(iteration, senderId) if balancingPhase >= 3 =>
      neighAckMsgFromStash(iteration) += senderId
      if (!receiveNeighAckMsgFrom(iteration).contains(senderId)) {
        receiveNeighAckMsgFrom(iteration) += senderId
      }
      if (!sendNeighAckMsgTo(iteration).contains(senderId)) {
        send(regionRef, senderId, AcknowledgeUpdateNeighbourhood(iteration, senderId))
        sendNeighAckMsgTo(iteration) += senderId
      }
      checkOrMoveToNextPhase(iteration)
      
    case AcknowledgeUpdateNeighbourhood(iteration, senderId) =>
      savedAcknowledgeUpdateNeighbourhood(iteration) :+= AcknowledgeUpdateNeighbourhood(iteration, senderId)
      //send if you got UN and AFN messages from workers you expected, send to all met node
      
    case SynchronizeBeforeStart(iteration, senderId) if balancingPhase >= 4 =>
      
      syncMsgFromStash(iteration) += senderId
      if (!receiveSyncMsgFrom(iteration).contains(senderId)) {
        receiveSyncMsgFrom(iteration) += senderId
      }
      if (!sendSyncMsgTo(iteration).contains(senderId)) {
        send(regionRef, senderId, SynchronizeBeforeStart(iteration, senderId))
        sendSyncMsgTo(iteration) += senderId
      }
      printAll(iteration, "S: " + balancingPhase)
      checkOrMoveToNextPhase(iteration)
      
    case SynchronizeBeforeStart(iteration, senderId) =>
      savedSynchronizeBeforeStart(iteration) :+= SynchronizeBeforeStart(iteration, senderId)
      
    case SynchronizeBeforeStart2(iteration, senderId) if balancingPhase == 5 =>
      sync2MsgFromStash(iteration) += senderId
      if (!receiveSync2MsgFrom(iteration).contains(senderId)) {
        receiveSync2MsgFrom(iteration) += senderId
      }
      if (!sendSync2MsgTo(iteration).contains(senderId)) {
        send(regionRef, senderId, SynchronizeBeforeStart2(iteration, senderId))
        sendSync2MsgTo(iteration) += senderId
      }
      printAll(iteration, "S2: " + balancingPhase)
      checkOrMoveToNextPhase(iteration)
      
    case SynchronizeBeforeStart2(iteration, senderId) =>
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
        if(currentIteration == 11){
        }
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
    logger.info("stash ;;; receive ;;; send\n" + additionMsg + "\n"
      + "N: " + neighMsgFromStash(iteration).toString() + ";;;" + receiveNeighMsgFrom(iteration).toString() + ";;;" + sendNeighMsgTo(iteration).toString() + "\n"
      + "NAck: " + neighAckMsgFromStash(iteration).toString() + ";;;" + receiveNeighAckMsgFrom(iteration).toString() + ";;;" + sendNeighAckMsgTo(iteration).toString() + "\n"
      + "FN: " + fixingNeighAckMsgFromStash(iteration).toString() + ";;;" + receiveFixingNeighAckMsgFrom(iteration).toString() + "\n"
      + "S1: " + syncMsgFromStash(iteration).toString() + ";;;" + receiveSyncMsgFrom(iteration).toString() + ";;;" + sendSyncMsgTo(iteration).toString() + "\n"
      + "S2: " + sync2MsgFromStash(iteration).toString() + ";;;" + receiveSync2MsgFrom(iteration).toString() + ";;;" + sendSync2MsgTo(iteration).toString() + "\n")
  }
  
  private def waitOrSendProposal(iteration: Long): Unit = {
    if (toTakeCellsFrom.isEmpty || toTakeCellsFrom.forall(i => i._2._2)) {
      if (toSendCellsTo.nonEmpty && !toSendCellsTo.forall(i => i._2._2)) {
        waitForProposeFrom = toSendCellsTo.find(i => !i._2._2).get._1
      }
    } else {
      val sendProposeTo = toTakeCellsFrom.find(i => !i._2._2)
      val amountParameter = 2
      val numberOfCells = ((1.0 - blockAvgTime / sendProposeTo.get._2._1) / amountParameter * worldShard.localCellIds.size).toInt
      send(regionRef, sendProposeTo.get._1, Proposal(iteration, this.id, numberOfCells))
      toTakeCellsFrom(sendProposeTo.get._1) = (sendProposeTo.get._2._1, true)
      sentProposeTo = sendProposeTo.get._1
    }
  }
  
  private def sendResignationToRestWorkers(iteration: Long): Unit = {
    val takeCellsKeys = toTakeCellsFrom.filter(n => !n._2._2).keys
    val sendCellsKeys = toSendCellsTo.filter(n => !n._2._2).keys
    val toSendResignation = (takeCellsKeys ++ sendCellsKeys).toSet
    distribute(mutable.Set.empty ++ toSendResignation, mutable.Map.empty ++ toSendResignation.map(n => n -> this.id).toMap)(this.id, id => Resignation(iteration, id))
    takeCellsKeys.foreach(key => toTakeCellsFrom(key) = (toTakeCellsFrom(key)._1, true))
    sendCellsKeys.foreach(key => toSendCellsTo(key) = (toSendCellsTo(key)._1, true))
  }
  
  private def addSenderToFutureReceiver(iteration: Long, senderId: WorkerId): Unit = {
    if (balancingPhase >= 3) {
      receiveNeighAckMsgFrom(iteration) += senderId
    }
    if (balancingPhase >= 4) {
      receiveSyncMsgFrom(iteration) += senderId
    }
    if (balancingPhase == 5) {
      receiveSync2MsgFrom(iteration) += senderId
    }
  }
  
  private def checkOrMoveToNextPhase(iteration: Long): Unit = {
    printAll(iteration, "check: " + balancingPhase)
    if(balancingPhase == 2
      && receiveNeighMsgFrom(iteration).size == neighMsgFromStash(iteration).size
      && receiveFixingNeighAckMsgFrom(iteration).size == fixingNeighAckMsgFromStash(iteration).size) {
      balancingPhase = 3
      receiveNeighAckMsgFrom(iteration) ++= (receiveNeighMsgFrom(iteration) ++ receiveFixingNeighAckMsgFrom(iteration))
      sendNeighAckMsgTo(iteration) ++= receiveNeighAckMsgFrom(iteration)
      sendNeighAckMsgTo(iteration).foreach(workerId => {
        send(regionRef, workerId, AcknowledgeUpdateNeighbourhood(iteration, this.id))
      })
      savedAcknowledgeUpdateNeighbourhood(iteration).foreach(aun => self ! aun)
      savedAcknowledgeUpdateNeighbourhood.remove(iteration)
    } 
    else if (balancingPhase == 3
      && receiveNeighAckMsgFrom(iteration).size == neighAckMsgFromStash(iteration).size
      && receiveFixingNeighAckMsgFrom(iteration).size == fixingNeighAckMsgFromStash(iteration).size) {
      balancingPhase = 4
      receiveSyncMsgFrom(iteration) ++= (receiveNeighMsgFrom(iteration) ++ receiveFixingNeighAckMsgFrom(iteration))
      sendSyncMsgTo(iteration) ++= receiveSyncMsgFrom(iteration)
      sendSyncMsgTo(iteration).foreach(workerId => {
        send(regionRef, workerId, SynchronizeBeforeStart(iteration, this.id))
      })
      savedSynchronizeBeforeStart(iteration).foreach(sbs => self ! sbs)
      savedSynchronizeBeforeStart.remove(iteration)
    } 
    else if (balancingPhase == 4
      && receiveNeighAckMsgFrom(iteration).size == neighAckMsgFromStash(iteration).size
      && receiveFixingNeighAckMsgFrom(iteration).size == fixingNeighAckMsgFromStash(iteration).size
      && receiveSyncMsgFrom(iteration).size == syncMsgFromStash(iteration).size) {
      balancingPhase = 5
      receiveSync2MsgFrom(iteration) ++= (receiveNeighMsgFrom(iteration) ++ receiveFixingNeighAckMsgFrom(iteration) ++ receiveSyncMsgFrom(iteration))
      sendSync2MsgTo(iteration) ++= receiveSync2MsgFrom(iteration)
      sendSync2MsgTo(iteration).foreach(workerId => {
        send(regionRef, workerId, SynchronizeBeforeStart2(iteration, this.id))
      })
      savedSynchronizeBeforeStart2(iteration).foreach(sbs2 => self ! sbs2)
      savedSynchronizeBeforeStart2.remove(iteration)
    } 
    else if (balancingPhase == 5
      && receiveNeighAckMsgFrom(iteration).size == neighAckMsgFromStash(iteration).size
      && receiveFixingNeighAckMsgFrom(iteration).size == fixingNeighAckMsgFromStash(iteration).size
      && receiveSyncMsgFrom(iteration).size == syncMsgFromStash(iteration).size
      && receiveSync2MsgFrom(iteration).size == sync2MsgFromStash(iteration).size) {
      //TODO: reset state
      if (bestWorkerWhoProposed != defValueWorker) {
        WorldCorrectnessChecker.checkIteration(iteration)
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
      bestWorkerWhoProposed = defValueWorker
      numberOfCellsToGive = 0
      cellsToTransferReceive = null
      balancingPhase = 0
      logger.info("ENDED BALANCING")
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
    sendNeighMsgTo(iteration).foreach(workerId => {
      if(iteration == 20){
        logger.info("dupa")
      }
      val workerOutCells = outCells.getOrElse(workerId, ImSet.empty)
      val workerInCells = inCells.getOrElse(workerId, ImSet.empty)
      val workerOutToRemove = cellsToTransferReceive.neighboursOutgoingCellsToRemove.getOrElse(workerId, ImSet.empty)
      if (workerId == this.id || workerId == cellsToTransferReceive.workerId || workerOutCells.isEmpty && workerInCells.isEmpty && workerOutToRemove.isEmpty) {
        send(regionRef, workerId, UpdateNeighbourhoodEmpty(iteration, this.id))
      } else {
        send(regionRef, workerId, UpdateNeighbourhood(iteration, this.id, cellsToTransferReceive.workerId, workerOutCells, workerOutToRemove, workerInCells))
      }
    })
  }
  
  private def distributeEmptyNeighUpdate(iteration: Long): Unit = {
    sendNeighMsgTo(iteration).foreach { workerId =>
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
      0.0, { time => Statistics(currentIteration, senderId, new StatisticsData(time)) })
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

  private def logMetrics(iteration: Long, metrics: Metrics): Unit = {
    logger.info(WorkerActor.MetricsMarker, "{};{}", iteration.toString, metrics: Any)
  }

  private def flatGroup[A](seqs: Seq[Seq[A]])(idExtractor: A => CellId): Map[CellId, Seq[A]] = {
    seqs.flatten.groupBy {
      idExtractor(_)
    }.to(Map)
  }

  private def shuffleUngroup[*, V](groups: Map[*, Seq[V]]): Seq[V] = {
    Random.shuffle(groups.keys.toList).flatMap(k => Random.shuffle(groups(k)))
  }
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

  final case class AcknowledgeFixNeighbourhood private(iteration: Long, senderId: WorkerId)

  final case class SynchronizeBeforeStart private(iteration: Long, senderId: WorkerId)

  final case class SynchronizeBeforeStart2 private(iteration: Long, senderId: WorkerId)

}