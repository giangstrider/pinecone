package reconciliation

import reconciliation.QueryStage.{ExecutedQuery, PrepareQuery, ReconciliationQuery}

object QueryReconciliation {
	def reconcile(source: ExecutedQuery, target: ExecutedQuery, query: PrepareQuery): ReconciliationQuery = {
		(source.result, target.result) match {
			case (None, None) => ReconciliationQuery(query.queryKey, query, None)
			case v@(_, _) =>
				val records = Some(RecordReconciliation.reconcile(v._1, v._2, query))
				ReconciliationQuery(query.queryKey, query, records)
		}
	}
}