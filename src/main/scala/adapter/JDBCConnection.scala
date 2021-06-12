package adapter

import anorm.Macro.ColumnNaming.SnakeCase

import java.sql.Connection
import java.util.Properties
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import anorm.{BatchSql, NamedParameter, RowParser, SQL, ToParameterList}
import configuration.Pinecone.{pineconeConf, connectionConf}


object JDBCConnection {
	private var connectionDictionary: Map[String, JDBCConnection] = Map.empty

	def getJdbcConnection(connectionName: String): JDBCConnection = {
		connectionDictionary.get(connectionName) match {
			case Some(jdbcConnection) => jdbcConnection
			case None =>
				val connectionJdbc = new JDBCConnection(getConnectionDetail(connectionName))
				connectionDictionary += (connectionName -> connectionJdbc)
				connectionJdbc
		}
	}

	private def getConnectionDetail(connectionName: String): Map[String, String] = {
		connectionConf.getOrElse(connectionName,
			throw new Exception(s"Connection $connectionName not found in connection.conf file"))
	}
}

class JDBCConnection(val config: Map[String, String]) {
	private val getDriver: String = {
		val databaseName = config("jdbcUrl").split(":")(1)
		pineconeConf.databaseSupportedDriver.getOrElse(databaseName,
			throw new Exception(s"Database $databaseName not supported by Pinecone yet"))
	}
	private var initializedConnection = None: Option[Connection]
	
	private implicit def getConnection: Connection = {
		initializedConnection match {
			case Some(connection) => connection
			case None =>
				val connection = initConnection
				initializedConnection = Some(connection)
				connection
		}
	}

	private def initConnection: Connection = {
		val properties = new Properties
		config.map(c => properties.put(c._1, c._2))
		properties.put("driverClassName", getDriver)
		val hikariConfig = new HikariConfig(properties)
		val dataSource = new HikariDataSource(hikariConfig)
		dataSource.getConnection
	}

	def executeQuery[R](query: String, parser: RowParser[R]): List[R] = {
		val sqlResult = SQL(query).executeQuery
		sqlResult.statementWarning match {
			case Some(warning) =>
				warning.printStackTrace()
				sqlResult.as(parser.*)
			case _ => sqlResult.as(parser.*)
		}
	}

	def executeInsert[R](tableName: String, macroParam: ToParameterList[R], data: List[R])	= {
		val dataParams = data.map(macroParam).zipWithIndex.map{
			case(row, i) => row.map(r => NamedParameter(s"${SnakeCase(r.name)}_$i", r.value))
		}
		val columns = dataParams.head.map(column => column.name.split("_").dropRight(1).mkString("_")).mkString(", ")
		val placeholders = dataParams.map(row => s"(${row.map(n => s"{${n.name}}").mkString(",")})").mkString(",")

		val placeholderQuery = s"INSERT INTO $tableName($columns) VALUES $placeholders"
		SQL(placeholderQuery).on(dataParams.flatten: _*).executeUpdate
	}

	def executeUpdateById[R](tableName: String, macroParam: ToParameterList[R], data: List[R], compositeKey: Set[String])	= {
		val dataParams = data.map(macroParam).map(row => row.map(r => NamedParameter(SnakeCase(r.name), r.value)))

		val columns = dataParams.head.map(_.name)
		val updatingColumns = columns.filterNot(compositeKey).map(column => s"$column = {$column}").mkString(", ")
		val compositeKeyColumns = compositeKey.map(column => s"$column = {$column}").mkString(" AND ")

		val placeholderQuery = s"UPDATE $tableName SET $updatingColumns WHERE $compositeKeyColumns"
		BatchSql(placeholderQuery, dataParams.head, dataParams.tail:_*).execute
	}
}
