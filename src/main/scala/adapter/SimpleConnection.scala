package adapter

import configuration.Pinecone.pineconeConf


class SimpleConnection(override val config: Map[String, String]) extends GeneralConnection(config) {
	protected def getDriver: String = {
		val databaseName = config("jdbcUrl").split(":")(1)
		pineconeConf.databaseSupportedDriver.getOrElse(databaseName,
			throw new Exception(s"Database $databaseName not supported by Pinecone yet"))
	}
}