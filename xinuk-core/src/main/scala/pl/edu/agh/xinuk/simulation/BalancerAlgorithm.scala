package pl.edu.agh.xinuk.simulation

import pl.edu.agh.xinuk.model.{WorkerId, WorldShard}

import scala.collection.mutable

class BalancerAlgorithm(
                    val worldShard: WorldShard){

  val neighboursPlanAvgTime: mutable.Map[WorkerId, Double] = worldShard.outgoingWorkerNeighbours
    .zip(List.fill(worldShard.outgoingWorkerNeighbours.size)(0.0)).to(mutable.Map)
}
