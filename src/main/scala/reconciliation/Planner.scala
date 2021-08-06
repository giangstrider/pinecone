package reconciliation

import anorm.SqlParser
import reconciliation.QueryStage.{ExecutionQuery, PrepareQuery}
import exception.PineconeExceptionHandler.exceptionStop


object Planner {
	private def prepareToExecution(preparedQueries: List[PrepareQuery]): List[ExecutionQuery] = {
		val executionQueries = for {preparedQuery <- preparedQueries} yield {
			List(ExecutionQuery(
				preparedQuery.queryKey, preparedQuery.sourceName, preparedQuery.sourceQuery, false, preparedQuery.reconcileKey
			),
				ExecutionQuery(
					preparedQuery.queryKey, preparedQuery.targetName, preparedQuery.targetQuery, true, preparedQuery.reconcileKey)
			)
		}
		executionQueries.flatten
	}

	def pickPrepareStrategy(strategy: String, preparedQueries: List[PrepareQuery]): List[List[ExecutionQuery]] = {
		val executionQuery = prepareToExecution(preparedQueries)
		strategy match {
			case "pair" => executionQuery.groupBy(_.queryKey).values.toList
			case "connection" => executionQuery.groupBy(_.connectionName).values.toList
			case _ => exceptionStop("No valid strategy found")
		}
	}
}