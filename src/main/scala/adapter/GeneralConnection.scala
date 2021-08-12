package adapter

import anorm.Macro.ColumnNaming.SnakeCase

import java.sql.Connection
import java.util.Properties
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import anorm.{BatchSql, NamedParameter, RowParser, SQL, ToParameterList}
import com.typesafe.scalalogging.LazyLogging
import configuration.Pinecone.connectionConf
import exception.PineconeExceptionHandler.exceptionStop
import reconciliation.QueryStage.{ExecutedQuery, ExecutionQuery}

import scala.util.{Failure, Success, Try}


abstract class GeneralConnection(val config: Map[String, String]) extends PineconeExecutionUtility with LazyLogging {
	protected implicit val connection: Connection = getConnection

	protected def getDriver: String

	private def getConnection: Connection = {
		initConnection match {
			case Success(c) => logger.info(s"Connection '${config("jdbcUrl")}' init successfully"); c
			case Failure(exception) => exceptionStop(exception)
		}
	}

	private def initConnection: Try[Connection] = {
		val properties = new Properties
		config.map(c => properties.put(c._1, c._2))
		properties.put("driverClassName", getDriver)
		Try {
			val hikariConfig = new HikariConfig(properties)
			val dataSource = new HikariDataSource(hikariConfig)
			dataSource.getConnection
		}
	}

	def executeQuery[R](query: String, parser: RowParser[R]): Try[List[R]] = {
		Try {
			val sqlResult = SQL(query).executeQuery
			sqlResult.statementWarning match {
				case Some(warning) =>
					warning.printStackTrace()
					sqlResult.as(parser.*)
				case _ => sqlResult.as(parser.*)
			}
		}
	}

	def executeUpdate(query: String): Try[Unit] = Try { SQL(query).executeUpdate }

	def executeInsert[R](tableName: String, macroParam: ToParameterList[R], data: List[R]): Try[Unit] = {
		val dataParams = data.map(macroParam).zipWithIndex.map{
			case(row, i) => row.map(r => NamedParameter(s"${SnakeCase(r.name)}_$i", r.value))
		}
		val columns = dataParams.head.map(column => column.name.split("_").dropRight(1).mkString("_")).mkString(", ")
		val placeholders = dataParams.map(row => s"(${row.map(n => s"{${n.name}}").mkString(",")})").mkString(",")

		val placeholderQuery = s"INSERT INTO $tableName($columns) VALUES $placeholders"

		Try(SQL(placeholderQuery).on(dataParams.flatten: _*).executeUpdate)
	}

	def executeUpdateById[R](tableName: String, macroParam: ToParameterList[R],
	                         data: List[R], compositeKey: Set[String]): Try[Array[Int]] = {
		val dataParams = data.map(macroParam).map(row => row.map(r => NamedParameter(SnakeCase(r.name), r.value)))

		val columns = dataParams.head.map(_.name)
		val updatingColumns = columns.filterNot(compositeKey).map(column => s"$column = {$column}").mkString(", ")
		val compositeKeyColumns = compositeKey.map(column => s"$column = {$column}").mkString(" AND ")

		val placeholderQuery = s"UPDATE $tableName SET $updatingColumns WHERE $compositeKeyColumns"
		Try(BatchSql(placeholderQuery, dataParams.head, dataParams.tail:_*).execute)
	}

	def executePineconeQuery(query: ExecutionQuery): ExecutedQuery = {
		getQueryResult(getResultSet(query.query), query)
	}

	def closeConnection(): Unit = connection.close; logger.info(s"Connection '${config("jdbcUrl")}' closed")
}

object GeneralConnection {
	private var connectionDictionary: Map[String, GeneralConnection] = Map.empty

	def getJdbcConnection(connectionName: String): GeneralConnection = {
		connectionDictionary.get(connectionName) match {
			case Some(jdbcConnection) => jdbcConnection
			case None =>
				val connectionDetail = getConnectionDetail(connectionName)
				val connectionJdbc = new SimpleConnection(connectionDetail)
				connectionDictionary += (connectionName -> connectionJdbc)
				connectionJdbc
		}
	}

	def getSnowflakeConnection(connectionName: String): SnowflakeConnection = {
		connectionDictionary.get(connectionName) match {
			case Some(jdbcConnection) => jdbcConnection.asInstanceOf[SnowflakeConnection]
			case None =>
				val connectionDetail = getConnectionDetail(connectionName)
				val connectionJdbc = new SnowflakeConnection(connectionDetail)
				connectionDictionary += (connectionName -> connectionJdbc)
				connectionJdbc
		}
	}

	def getDatabaseType(connectionName: String): String = getConnectionDetail(connectionName)("jdbcUrl").split(":")(1)

	private def getConnectionDetail(connectionName: String): Map[String, String] = {
		connectionConf.getOrElse(connectionName,
			throw new Exception(s"Connection $connectionName not found in connection.conf file"))
	}
}

