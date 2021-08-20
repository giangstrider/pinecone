package configuration


case class StageConf(
    stageKey: String,
    connectionName: String,
    query: SQLMethod,
    isOriginal: Boolean
)

case class StagesConf(
    stages: List[StageConf]
)


case class MultiStagesWorkflowConf(
	workflowKey: String,
	stages: String,
	reconcileKeys: String,
	acceptedDeviation: Double,
    canEmpty: Boolean,
    maximumEmptyOnConsecutiveTimes: Int
)

case class MultiStagesWorkflowsConf(
    workflows: List[MultiStagesWorkflowConf]
)
