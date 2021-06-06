package reconciliation

import adapter.JDBCConnection
import anorm.Macro, Macro.ColumnNaming
import configuration.Pinecone.{pineconeConf, connectionConf}


case class ReconciliationQuery(queryKey: String, sourceName: String, sourceQuery: String, targetQuery: String)


object Controller {
	private val reconControlParser = Macro.namedParser[ReconciliationQuery](ColumnNaming.SnakeCase)
	private val conf = pineconeConf
	private val reconControlQueries: String = {
		s"SELECT * FROM ${conf.database.databaseName}.${conf.database.schemaName}.RECON_QUERIES"
	}
	private val reconDatabaseDetail: Map[String, String] = {
		connectionConf.getOrElse(conf.database.connectionName,
			throw new Exception(s"Connection ${conf.database.connectionName} not found in connection.conf file"))
	}

	def getQueries: List[ReconciliationQuery] = {
		new JDBCConnection(reconDatabaseDetail).executeQuery(reconControlQueries, reconControlParser)
	}
}
