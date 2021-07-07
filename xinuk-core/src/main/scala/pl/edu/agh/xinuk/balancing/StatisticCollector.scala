package pl.edu.agh.xinuk.balancing

class StatisticCollector {
  private var archivalBlockCounter: Integer = 0
  private var avgArchivalBlock: StatisticsData = _
  private var lastBlock: StatisticsData = _
  private var actualBlock: StatisticsData = _
  
  def addStatisticsDataBlock(dataBlock: StatisticsData) = {
    if(actualBlock == null) {
      actualBlock = dataBlock
    } else if(lastBlock == null) {
      lastBlock = actualBlock
      actualBlock = lastBlock
    } else if(avgArchivalBlock == null) {
      avgArchivalBlock = lastBlock
      archivalBlockCounter += 1
      lastBlock = actualBlock
      actualBlock = dataBlock
    } else {
      val time = avgArchivalBlock.avgPlanBlockTime * archivalBlockCounter + lastBlock.avgPlanBlockTime
      archivalBlockCounter += 1
      val avgTime = time / archivalBlockCounter
      avgArchivalBlock = new StatisticsData(avgTime)
      lastBlock = actualBlock
      actualBlock = dataBlock
    }
  }
}
