package reconciliation


case class QueryRecord (
	columns: List[QueryColumn],
	reconcileKeyValue: String
)

case class QueryColumn (
    columnName: String,
    columnValue: Option[Any],
    metadata: Option[QueryMetadataColumn]
)

case class QueryMetadataColumn (
	displaySize: Int,
	precision: Int,
	scale: Int,
	isCurrency: Boolean,
	isNullable: Boolean
)

case class ReconciledColumn (
    sourceColumnName: String,
    targetColumnName: String,
    isReconcileKey: Boolean,
    value: ReconcileTypedColumn,
    metadata: Option[ReconcileMetadataColumn]
)

sealed trait QueryStage {
	def queryKey: String
}

object QueryStage {
	case class PrepareQuery(
		queryKey: String, workflowKey: String, sourceName: String, sourceQuery: String, targetName: String, targetQuery: String,
		acceptedDeviation: Double, reconcileKey: List[String]) extends QueryStage

	case class ExecutionQuery private[QueryStage](queryKey: String, connectionName: String, query: String,
	                                              isTarget: Boolean, reconcileKey: List[String]) extends QueryStage

	case class ExecutedQuery private[QueryStage](queryKey: String, result: Option[List[QueryRecord]],
	                                             isTarget: Boolean) extends QueryStage

	case class ReconciliationRecord private[QueryStage](queryKey: String, reconciled: List[ReconciledColumn]) extends QueryStage

	case class ReconciliationQuery private[QueryStage](queryKey: String, query: PrepareQuery, records: Option[List[ReconciliationRecord]]) extends QueryStage

	def apply(queryKey: String, workflowKey: String, sourceName: String, sourceQuery: String, targetName: String, targetQuery: String,
	          acceptedDeviation: Double, reconcileKey: List[String]) =
		PrepareQuery(queryKey, workflowKey, sourceName, sourceQuery, targetName, targetQuery, acceptedDeviation, reconcileKey)

	def apply(queryKey: String, connectionName: String, query: String, isTarget: Boolean, reconcileKey: List[String]) =
		ExecutionQuery(queryKey, connectionName,query, isTarget, reconcileKey)

	def apply(queryKey: String, result: Option[List[QueryRecord]], isTarget: Boolean) = ExecutedQuery(queryKey, result, isTarget)

	def apply(queryKey: String, reconciled: List[ReconciledColumn]) =
		ReconciliationRecord(queryKey, reconciled)

	def apply(queryKey: String, query: PrepareQuery, records: Option[List[ReconciliationRecord]]) =
		ReconciliationQuery(queryKey, query, records)
}