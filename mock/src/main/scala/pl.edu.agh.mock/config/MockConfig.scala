package pl.edu.agh.mock.config

import pl.edu.agh.xinuk.config.{GuiType, MetricFunType, TestCaseType, XinukConfig}
import pl.edu.agh.xinuk.model.{Signal, WorldType}

final case class MockConfig(worldType: WorldType,
                            worldWidth: Int,
                            worldHeight: Int,
                            iterationsNumber: Long,

                            signalSuppressionFactor: Double,
                            signalAttenuationFactor: Double,
                            signalSpeedRatio: Int,

                            workersRoot: Int,
                            isSupervisor: Boolean,
                            shardingMod: Int,

                            guiType: GuiType,
                            guiCellSize: Int,

                            mockInitialSignal: Signal,

                            override val isDebugMode: Boolean,
                            override val shouldBalance: Boolean,
                            override val statisticsDistributionInterval: Int,
                            override val balancingIntervalMultiplier: Int,
                            override val shouldGoDepth: Boolean,
                            override val shouldUseMetricOnAllFoundCells: Boolean,
                            override val shouldUpdateMiddlePoint: Boolean,
                            override val shouldCheckCorrectness: Boolean,
                            override val shouldLogChanges: Boolean,
                            override val minimumNumberOfAdjacentCells: Int,
                            override val minimumDiffBetweenWorkers: Double,
                            override val amountCellsDivider: Int,
                            override val balancingMessageDelay: Int,
                            override val slowMultiplier: Double,
                            override val simulationCase: TestCaseType,
                            override val metricFunction: MetricFunType
                           ) extends XinukConfig