package reconciliation


case class QueryRecord (
	columns: List[QueryColumn],
	reconcileKeyValue: String
)

case class QueryColumn (
    columnName: String,
    columnValue: Option[Any],
    jvmClassName: String,
    nullable: Boolean = true
)

case class ReconcileKeyColumn (
    columnName: String,
    columnValue: String
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

	case class ExecutedQuery private[QueryStage](queryKey: String, result: Option[List[QueryRecord]], isTarget: Boolean) extends QueryStage

	case class ReconciliationRecord private[QueryStage](queryKey: String, reconcileKeys: List[ReconcileKeyColumn],
	                                                    reconciled: List[ReconcileTypedColumn]
	                                                   ) extends QueryStage

	def apply(queryKey: String, sourceName: String, sourceQuery: String, targetName: String, targetQuery: String,
	          acceptedDeviation: Double, reconcileKey: List[String]) =
		PrepareQuery(queryKey, sourceName, sourceQuery, targetName, targetQuery, acceptedDeviation, reconcileKey)

	def apply(queryKey: String, connectionName: String, query: String, isTarget: Boolean, reconcileKey: List[String]) =
		ExecutionQuery(queryKey, connectionName,query, isTarget, reconcileKey)

	def apply(queryKey: String, result: Option[List[QueryRecord]], isTarget: Boolean) = ExecutedQuery(queryKey, result, isTarget)

	def apply(queryKey: String, reconcileKeys: List[ReconcileKeyColumn], reconciled: List[ReconcileTypedColumn]) =
		ReconciliationRecord(queryKey, reconcileKeys, reconciled)
}
