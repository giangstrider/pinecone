package reconciliation

import adapter.ReconController
import configuration.Pinecone.pineconeConf


class Executor {
	private val strategy = pineconeConf.reconciliation.strategy

	def execute = {
		val executionQueries = Planner.pickPrepareStrategy(strategy, ReconController.getQueries)
	}
}
