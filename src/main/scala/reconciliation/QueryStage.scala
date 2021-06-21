package reconciliation


case class QueryRecord (
	columns: List[QueryColumn],
	reconcileKeyValue: String
)

case class QueryColumn (
    columnName: String,
    columnValue: Option[Any]
)

case class ReconcileColumn (
    columnName: String,
    sourceValue: Option[Any],
    targetValue: Option[Any],
    different: Double,
    deviation: Double
)

sealed trait QueryStage {
	def queryKey: String
}

object QueryStage {
	case class PrepareQuery(
		queryKey: String, sourceName: String, sourceQuery: String, targetName: String, targetQuery: String,
		acceptedDeviation: Double, reconcileKey: List[String]) extends QueryStage

	case class ExecutionQuery private[QueryStage](queryKey: String, connectionName: String, query: String,
	                                              isTarget: Boolean, reconcileKey: List[String]) extends QueryStage

	case class ExecutedQuery private[QueryStage](queryKey: String, result: List[QueryRecord], isTarget: Boolean) extends QueryStage

	case class ReconciliationRecord private[QueryStage](queryKey: String, reconcileKeyValue: String,
	                                                   reconciliation: List[ReconcileColumn]) extends QueryStage

	def apply(queryKey: String, sourceName: String, sourceQuery: String, targetName: String, targetQuery: String,
	          acceptedDeviation: Double, reconcileKey: List[String]): PrepareQuery =
		PrepareQuery(queryKey: String, sourceName: String, sourceQuery: String, targetName: String, targetQuery: String,
			acceptedDeviation: Double, reconcileKey: List[String])

	def apply(queryKey: String, connectionName: String, query: String, isTarget: Boolean, reconcileKey: List[String]): ExecutionQuery =
		ExecutionQuery(queryKey: String, connectionName: String,query: String, isTarget: Boolean, reconcileKey: List[String])

	def apply(queryKey: String, result: List[QueryRecord], isTarget: Boolean): ExecutedQuery = ExecutedQuery(queryKey: String, result: List[QueryRecord], isTarget: Boolean)

	def apply(queryKey: String, reconcileKeyValue: String, reconciliation: List[ReconcileColumn]): ReconciliationRecord =
		ReconciliationRecord(queryKey: String, reconcileKeyValue: String, reconciliation: List[ReconcileColumn])
}
