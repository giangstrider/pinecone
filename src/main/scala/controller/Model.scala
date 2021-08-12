package controller


case class Stage(
	stageKey: String,
	query: String,
	isOriginal: Boolean
)


case class SingleStageWorkflow(
    workflowKey: String,
    stage: Stage,
    canEmpty: Boolean,
    canEmptyOnConsecutiveTimes: Boolean,
    maximumEmptyOnConsecutiveTimes: Int
)


case class MultiStagesWorkflow(
    workflowKey: String,
    stages: List[Stage],
    reconcileKeys: List[String],
	acceptedDeviation: Double
)
