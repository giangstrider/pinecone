package controller

import adapter.GeneralConnection
import anorm.RowParser
import com.typesafe.scalalogging.LazyLogging
import configuration.{MultiStagesWorkflowConf, SingleStageWorkflowConf}
import exception.PineconeExceptionHandler.sqlException
import reconciliation.QueryStage.PrepareQuery


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

	def transformMultiStages(data: List[MultiStagesWorkflowConf], stages: List[Stage]): List[PrepareQuery] = {
		data.flatMap { w =>
			val keys = w.stages.split(",")
			val filterStages = groupStageByKey(stages).filter(ws => keys.contains(ws._1)).values.flatten.toList
			val fixedFirstStage = filterStages.filter(_.isOriginal).head // should use head-option instead
			val targetedStages = filterStages.filterNot(_.isOriginal)
			targetedStages.map({ t =>
				val formedQueryKey = s"${w.workflowKey}_${fixedFirstStage.stageKey}_${t.stageKey}"
				PrepareQuery(formedQueryKey, w.workflowKey, fixedFirstStage.connectionName, fixedFirstStage.query, t.connectionName, t.query, w.acceptedDeviation, w.reconcileKeys.split(",").toList)
			})
		}
	}

	private def groupStageByKey(stages: List[Stage]) = stages.groupBy(_.stageKey)
}

