package adapter

import configuration.Pinecone.pineconeConf
import reconciliation.QueryStage.{ExecutedQuery, ExecutionQuery}


class SimpleConnection(override val config: Map[String, String]) extends GeneralConnection(config) with PineconeExecutionUtility {
	protected def getDriver: String = {
		val databaseName = config("jdbcUrl").split(":")(1)
		pineconeConf.databaseSupportedDriver.getOrElse(databaseName,
			throw new Exception(s"Database $databaseName not supported by Pinecone yet"))
	}

	def getPineconeExecutedQuery(queries: List[ExecutionQuery]): List[ExecutedQuery] = {
		queries.map(q => getQueryResult(getResultSet(q.query), q))
	}
}