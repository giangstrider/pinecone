package adapter

import anorm.{Macro, RowParser, SqlParser}
import anorm.SqlParser._
import anorm.Column.{ columnToArray }
import anorm.Macro.ColumnNaming.SnakeCase
import anorm.~
import configuration.Pinecone.pineconeConf
import exception.PineconeExceptionHandler.exceptionStop
import reconciliation.QueryStage.PrepareQuery

import scala.util.{Failure, Success}


object ReconController {
//	private val reconControlParser = Macro.namedParser[PrepareQuery](SnakeCase)
	private val reconControlParser: RowParser[PrepareQuery] =
		str("QUERY_KEY") ~ str("SOURCE_NAME") ~ str("SOURCE_QUERY") ~ str("TARGET_NAME") ~ get[String]("TARGET_QUERY") ~ double("ACCEPTED_DEVIATION") map {
			case a ~ b ~ c ~ d  ~e ~ f  => PrepareQuery(a, b, c, d, e, f, List("TradingDate"))
		}

	private val p = str("name") ~ int("population") map { case n ~ p  => (n, p) }

	private val connection = GeneralConnection.getSnowflakeConnection(pineconeConf.database.connectionName)
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