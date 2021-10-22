package interface

import configuration.MultiStagesWorkflowConf
import controller.ApplicationController
import reconciliation.Executor

import scala.util.Success

object Command {
	object Reconciliation {
		def reconcile = {
			val workflows = ApplicationController.getWorkflows
			val queries = ApplicationController.getQueries(workflows)
			val fReconciledQuery = Executor.execute(queries)
			ApplicationController.insertResults(fReconciledQuery)
//			val groupedWorkflows = workflows.groupBy(_.workflowKey)
//			val x: String = fReconciledQuery.transform {
//				case Success(qs) => qs.map{ qsr =>
//					qsr.records match {
//						case Some(v) => false
//						case None => false
//					}
//				}
//			}
		}
	}

}
