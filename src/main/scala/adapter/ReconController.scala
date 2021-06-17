package adapter

import anorm.Macro
import anorm.Macro.ColumnNaming.SnakeCase
import configuration.Pinecone.pineconeConf
import exception.PineconeExceptionHandler.exceptionStop
import reconciliation.QueryStage.PrepareQuery
import scala.util.{Failure, Success}


object ReconController {
	private val reconControlParser = Macro.namedParser[PrepareQuery](SnakeCase)
	private val connection = GeneralConnection.getJdbcConnection(pineconeConf.database.connectionName)
	private val fullyQualifiedTableName =
		s"${pineconeConf.database.databaseName}.${pineconeConf.database.schemaName}.${pineconeConf.database.queriesTableName}"
	private val reconCompositeKey: Set[String] = Set("query_key", "source_name", "target_name")
	private val reconControlQueries = s"SELECT * FROM $fullyQualifiedTableName"

	def getQueries: List[PrepareQuery] = {
		connection.executeQuery(reconControlQueries, reconControlParser) match {
			case Success(value) => value
			case Failure(exception) => exceptionStop(exception)
		}
	}

	def insert(list: List[PrepareQuery]) = {
		val toParams = Macro.toParameters[PrepareQuery]
		connection.executeInsert(fullyQualifiedTableName, toParams, list)
	}

	def update(list: List[PrepareQuery]) = {
		val toParams = Macro.toParameters[PrepareQuery]
		connection.executeUpdateById(fullyQualifiedTableName, toParams, list, reconCompositeKey)
	}
}