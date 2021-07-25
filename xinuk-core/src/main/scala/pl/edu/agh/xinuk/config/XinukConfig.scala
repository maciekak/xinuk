package pl.edu.agh.xinuk.config

import com.avsystem.commons.misc.{AbstractNamedEnumCompanion, NamedEnum}
import pl.edu.agh.xinuk.model.WorldType

trait XinukConfig {
  def worldType: WorldType
  def worldWidth: Int
  def worldHeight: Int
  def iterationsNumber: Long

  def signalSuppressionFactor: Double
  def signalAttenuationFactor: Double
  def signalSpeedRatio: Int

  def workersRoot: Int
  def isSupervisor: Boolean
  def shardingMod: Int

  def guiType: GuiType
  def guiCellSize: Int

  def isDebugMode: Boolean = false
  def shouldBalance: Boolean = false
  def statisticsDistributionInterval: Int = 10
  def balancingIntervalMultiplier: Int = 1
  def shouldGoDepth: Boolean = true
  def shouldUseMetricOnAllFoundCells: Boolean = false
  def shouldUpdateMiddlePoint: Boolean = false
  def shouldCheckCorrectness: Boolean = false
  def shouldLogChanges: Boolean = false
  def minimumNumberOfAdjacentCells: Int = 9
  def minimumDiffBetweenWorkers: Double = 0.85
  def amountCellsDivider: Int = 6
  def balancingMessageDelay: Int = 3

  def slowMultiplier: Double = 0.7
  def simulationCase: TestCaseType = TestCaseType.None
  def metricFunction: MetricFunType
}

sealed trait GuiType extends NamedEnum

object GuiType extends AbstractNamedEnumCompanion[GuiType] {

  override val values: List[GuiType] = caseObjects

  case object None extends GuiType {
    override val name: String = "none"
  }

  case object Grid extends GuiType {
    override def name: String = "grid"
  }
}

sealed trait TestCaseType extends NamedEnum

object TestCaseType extends AbstractNamedEnumCompanion[TestCaseType] {

  override val values: List[TestCaseType] = caseObjects

  case object None extends TestCaseType {
    override def name: String = "none"
  }

  case object Mock extends TestCaseType {
    override def name: String = "mock"
  }

  case object WorkerCells extends TestCaseType {
    override def name: String = "workerCells"
  }

  case object EveryoneDifferentCellTime extends TestCaseType {
    override def name: String = "everyoneDifferentCellTime"
  }

  case object AreaMiddle extends TestCaseType {
    override def name: String = "areaMiddle"
  }

  case object AreaLongRect extends TestCaseType {
    override def name: String = "areaLongRect"
  }

  case object AreaHalf extends TestCaseType {
    override def name: String = "areaHalf"
  }

  case object AreaFreeBorders extends TestCaseType {
    override def name: String = "areaFreeBorders"
  }

  case object AreaSlowBorders extends TestCaseType {
    override def name: String = "areaSlowBorders"
  }

  case object AgentType extends TestCaseType {
    override def name: String = "agentType"
  }
}

sealed trait MetricFunType extends NamedEnum

object MetricFunType extends AbstractNamedEnumCompanion[MetricFunType] {

  override val values: List[MetricFunType] = caseObjects

  case object EuclidClose extends MetricFunType {
    override def name: String = "euclidClose"
  }

  case object EuclidFar extends MetricFunType {
    override def name: String = "euclidFar"
  }

  case object MaxMin extends MetricFunType {
    override def name: String = "maxMin"
  }
}
