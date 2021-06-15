package adapter

import configuration.Pinecone.pineconeConf


class SnowflakeConnection(override val config: Map[String, String]) extends GeneralConnection(config) {
	def getDriver: String =  pineconeConf.databaseSupportedDriver.getOrElse("snowflake",
			throw new Exception(s"Database snowflake not supported by Pinecone yet"))
}
