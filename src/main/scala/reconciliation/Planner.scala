package reconciliation

import anorm.SqlParser
import reconciliation.QueryStage.{ExecutionQuery, PrepareQuery}
import exception.PineconeExceptionHandler.exceptionStop


object Planner {
	private val reconQueriesParser = SqlParser.folder(Map.empty[String, Any]) { (map, value, meta) =>
		Right(map + (meta.column.alias.get -> value))
//		val s: String = value.getClass.
	}
//	private val reconQueriesParser2 = SqlParser.folder(List.empty[QueryRecord]) { (list, value, meta) =>
//		Right(list :+ QueryRecord(meta.column.alias.get, Some(value)))
//	}

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

	def pickPrepareStrategy(strategy: String, preparedQueries: List[PrepareQuery]): Map[String, List[ExecutionQuery]] = {
		val executionQuery = prepareToExecution(preparedQueries)
		strategy match {
			case "pair" => executionQuery.groupBy(_.queryKey)
			case "connection" => executionQuery.groupBy(_.connectionName)
			case _ => exceptionStop("No valid strategy found")
		}
	}
}
