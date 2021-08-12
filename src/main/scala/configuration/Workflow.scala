package configuration


case class StageConf(
    stageKey: String,
    query: SQLMethod,
    isOriginal: Boolean = false
)

case class StagesConf(
    stages: List[StageConf]
)


case class MultiStagesWorkflowConf(
	workflowKey: String,
	stages: String,
	reconcileKeys: String,
	acceptedDeviation: Double
)

case class MultiStagesWorkflowsConf(
    workflows: List[MultiStagesWorkflowConf]
)

case class SingleStageWorkflowConf(
    workflowKey: String,
    stage: String,
    canEmpty: Boolean,
    canEmptyOnConsecutiveTimes: Boolean,
    maximumEmptyOnConsecutiveTimes: Int
)

case class SingleStageWorkflowsConf(
    workflows: List[SingleStageWorkflowConf]
)