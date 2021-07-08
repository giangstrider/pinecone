package reconciliation

import reconciliation.QueryStage.{ExecutedQuery, PrepareQuery, ReconciliationQuery}
import exception.PineconeExceptionHandler.exceptionStop

object QueryReconciliation {
	def reconcile(source: ExecutedQuery, target: ExecutedQuery, query: PrepareQuery): ReconciliationQuery = {
		(source.result, target.result) match {
			case (None, None) => exceptionStop("Noti")
			case v@(_, _) =>
				val records = Some(RecordReconciliation.reconcile(v._1, v._2, query))
				ReconciliationQuery(query.queryKey, records)
		}
	}
}
