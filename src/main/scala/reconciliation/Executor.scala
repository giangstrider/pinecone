package reconciliation

import configuration.Pinecone.pineconeConf


class Executor {
	private val strategy = pineconeConf.reconciliation.strategy
}
