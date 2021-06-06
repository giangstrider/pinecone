package adapter

import java.sql.Connection
import java.util.Properties
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import anorm.{RowParser, SQL}
import configuration.Pinecone.pineconeConf


class JDBCConnection(val config: Map[String, String]) {
	private val supportedDriver = Map(
		"sqlserver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "db2" -> "com.ibm.db2.jcc.DB2Driver",
        "redshift" -> "com.amazon.redshift.jdbc.Driver",
        "postgresql" -> "org.postgresql.Driver",
        "snowflake" -> "net.snowflake.client.jdbc.SnowflakeDriver"
	)
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
}
