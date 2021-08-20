package controller

import adapter.GeneralConnection
import anorm.RowParser
import com.typesafe.scalalogging.LazyLogging
import configuration.MultiStagesWorkflowConf
import exception.PineconeExceptionHandler.sqlException
import reconciliation.QueryStage.PrepareQuery


object Loader extends LazyLogging {
	def load[S](query: String, parser: RowParser[S])(implicit connection: GeneralConnection): List[S] = {
		sqlException(connection.executeQuery(query, parser), s"Load ${query} successfully", logger)
	}

	def transformMultiStages(data: List[MultiStagesWorkflowConf], stages: Map[String, List[Stage]]): List[PrepareQuery] = {
		data.flatMap { w =>
			val keys = w.stages.split(",")
			val filterStages = stages.filter(ws => keys.contains(ws._1)).values.flatten.toList
			val fixedFirstStage = filterStages.filter(_.isOriginal).head // should use head-option instead
			val targetedStages = filterStages.filterNot(_.isOriginal)
			targetedStages.map({ t =>
				val formedQueryKey = s"${w.workflowKey}_${fixedFirstStage.stageKey}_${t.stageKey}"
				PrepareQuery(formedQueryKey, w.workflowKey, fixedFirstStage.connectionName, fixedFirstStage.query, t.connectionName, t.query, w.acceptedDeviation,
				w.reconcileKeys.split(",").toList)
			})
		}
	}
}

