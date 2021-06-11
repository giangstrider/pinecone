package adapter

import anorm.Macro.ColumnNaming.SnakeCase

import java.sql.Connection
import java.util.Properties
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import anorm.{BatchSql, Macro, NamedParameter, RowParser, SQL, SqlStringInterpolation, ToParameterList}
import configuration.Pinecone.pineconeConf


class JDBCConnection(val config: Map[String, String]) {
	private val getDriver: String = {
		val databaseName = config("jdbcUrl").split(":")(1)
		pineconeConf.databaseSupportedDriver.getOrElse(databaseName,
			throw new Exception(s"Database ${databaseName} not supported by Pinecone yet"))
	}

	def initConnection: Connection = {
		val properties = new Properties
		config.map(c => properties.put(c._1, c._2))
		properties.put("driverClassName", getDriver)
		val hikariConfig = new HikariConfig(properties)
		val dataSource = new HikariDataSource(hikariConfig)
		dataSource.getConnection
	}

	def executeQuery[R](query: String, parser: RowParser[R]): List[R] = {
		implicit val connection: Connection = initConnection
		val sqlResult = SQL(query).executeQuery
		sqlResult.statementWarning match {
			case Some(warning) =>
				warning.printStackTrace()
				sqlResult.as(parser.*)
			case _ => sqlResult.as(parser.*)
		}
	}

	def executeInsert[R](tableName: String, macroParam: ToParameterList[R], data: List[R]) = {
		implicit val connection: Connection = initConnection
		val dataParams = for {p <- data.flatMap(macroParam)} yield NamedParameter(SnakeCase(p.name), p.value)
		val columns = dataParams.map(column => column.name)
		val placeholders = columns.map {n => s"{$n}"} mkString ", "
		val transformQuery = s"INSERT INTO $tableName(${columns mkString ", "}) VALUES ($placeholders)"
		SQL(transformQuery).on(dataParams: _*).executeUpdate()
	}
}
