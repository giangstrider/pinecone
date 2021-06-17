package reconciliation


case class QueryResult (
    records: List[QueryRecord]
)

case class QueryRecord (
    columnName: String,
    columnValue: Option[Any]
)

sealed trait QueryStage {
	def queryKey: String
}

object QueryStage {
	case class PrepareQuery(
		queryKey: String, sourceName: String, sourceQuery: String,
		targetName: String, targetQuery: String, acceptedDeviation: Double) extends QueryStage

	case class ExecutionQuery private[QueryStage](queryKey: String, connectionName: String, query: String,
	                                              isTarget: Boolean) extends QueryStage

	case class ExecutedQuery private[QueryStage](queryKey: String, result: List[QueryResult], isTarget: Boolean)
		extends QueryStage

	case class ReconciliationQuery private[QueryStage](queryKey: String,
	    source: List[QueryResult], target: List[QueryResult], acceptedDeviation: Double) extends QueryStage

	def apply(queryKey: String, sourceName: String, sourceQuery: String,
		targetName: String, targetQuery: String, acceptedDeviation: Double): PrepareQuery =
		PrepareQuery(queryKey: String, sourceName: String, sourceQuery: String,
					targetName: String, targetQuery: String, acceptedDeviation: Double)

	def apply(queryKey: String, connectionName: String, query: String, isTarget: Boolean): ExecutionQuery =
		ExecutionQuery(queryKey: String, connectionName: String,query: String, isTarget: Boolean)

	def apply(queryKey: String, result: List[QueryResult], isTarget: Boolean): ExecutedQuery = ExecutedQuery(queryKey: String, result: List[QueryResult], isTarget: Boolean)

	def apply(queryKey: String, source: List[QueryResult], target: List[QueryResult], acceptedDeviation: Double)
	: ReconciliationQuery = ReconciliationQuery(queryKey: String, source: List[QueryResult], target: List[QueryResult], acceptedDeviation: Double)
}
