package pl.edu.agh.formin

import java.{lang => jl}

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.avsystem.commons._
import com.google.common.cache.CacheBuilder
import pl.edu.agh.formin.SchedulerActor.{IterationFinished, IterationPartFinished, StartSimulation, StopSimulation}
import pl.edu.agh.formin.model.Grid

import scala.collection.mutable

class SchedulerActor(workers: Vector[ActorRef]) extends Actor with ActorLogging {

  //todo: this could benefit from being totally unsynchronized, but was a fast way to get a bounded map
  private val iteration2status: mutable.Map[jl.Long, IterationStatus] =
    CacheBuilder.newBuilder()
      .maximumSize(100)
      .concurrencyLevel(1)
      .build[jl.Long, IterationStatus]().asMap().asScala

  private var iterations: Long = _

  override def receive: Receive = stopped

  def stopped: Receive = {
    case StartSimulation(iterations) =>
      this.iterations = iterations
      log.info("Simulation started, iterations={}", iterations)
      context.become(started)
  }

  def started: Receive = {
    case StopSimulation =>
      log.info("Simulation stopped.")
      context.become(stopped)
    case IterationPartFinished(iteration, status) =>
      iteration2status.getOpt(iteration - 1).foreach(_.remove(status.worker))
      iteration2status.getOpt(iteration) match {
        case Opt(currentIterationStatus) =>
          currentIterationStatus.add(status)
          if (currentIterationStatus.size == workers.size) self ! IterationFinished(iteration)
        case Opt.Empty =>
          log.warning("Cache miss on iteration {} part finish for worker {}", iteration, status.worker)
      }
    case IterationFinished(i) if i == iterations =>
      self ! StopSimulation
    case IterationFinished(i) =>
      val nextIteration = i + 1
      iteration2status.update(nextIteration, IterationStatus.empty())
      workers.foreach(_ ! WorkerActor.StartIteration(nextIteration))
  }
}

object SchedulerActor {

  case class StartSimulation(iterations: Long) extends AnyVal

  case class IterationPartFinished(iteration: Long, simulationStatus: SimulationStatus)

  case class IterationFinished(i: Long) extends AnyVal

  case object StopSimulation

}

case class IterationStatus private() {
  private val worker2grid = mutable.HashMap[WorkerId, Grid]()

  def add(status: SimulationStatus): Unit = {
    worker2grid += status.worker -> status.grid
  }

  def remove(id: WorkerId): Unit = {
    worker2grid.remove(id)
  }

  def size: Int = {
    worker2grid.size
  }

}

object IterationStatus {
  def empty(): IterationStatus = IterationStatus()
}

case class SimulationStatus(worker: WorkerId, grid: Grid)