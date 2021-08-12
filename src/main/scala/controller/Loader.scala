package controller

import adapter.GeneralConnection
import anorm.RowParser
import com.typesafe.scalalogging.LazyLogging
import configuration.{MultiStagesWorkflowConf, SingleStageWorkflowConf}
import exception.PineconeExceptionHandler.sqlException


object Loader extends LazyLogging {
	def load[S](query: String, parser: RowParser[S])(implicit connection: GeneralConnection): List[S] = {
		sqlException(connection.executeQuery(query, parser), s"Load ${query} successfully", logger)
	}

	def transformSingleStage(data: List[SingleStageWorkflowConf], stages: List[Stage]): List[SingleStageWorkflow] = {
		data.map({ w =>
			val stage = groupStageByKey(stages)(w.stage).head
			SingleStageWorkflow(
				w.workflowKey, stage, w.canEmpty, w.canEmptyOnConsecutiveTimes, w.maximumEmptyOnConsecutiveTimes
			)
		})
	}

	def transformMultStages(data: List[MultiStagesWorkflowConf], stages: List[Stage]): List[MultiStagesWorkflow] = {
		data.map({ w =>
			val keys = w.stages.split(",")
			val stages = groupStageByKey(stages).filter(ws => keys.contains(ws._1)).values.flatten.toList
			MultiStagesWorkflow(
				w.workflowKey, stages, w.reconcileKeys.split(",").toList, w.acceptedDeviation
			)
		})
	}

	private def groupStageByKey(stages: List[Stage]) = stages.groupBy(_.stageKey)
}

