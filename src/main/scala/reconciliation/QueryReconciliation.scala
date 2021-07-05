package reconciliation

import reconciliation.QueryStage.{ExecutedQuery, PrepareQuery, ReconciliationQuery}
import exception.PineconeExceptionHandler.exceptionStop

object QueryReconciliation {
	def reconcile(source: ExecutedQuery, target: ExecutedQuery, query: PrepareQuery): List[ReconciliationQuery] = {
		val reconciledRecords = (source.result, target.result) match {
			case (None, None) => exceptionStop("Noti")
			case v@(_, _) => RecordReconciliation.reconcile(v._1, v._2, query)
		}
	}
}
