package adapter

import configuration.Pinecone.pineconeConf
import net.snowflake.client.core.QueryStatus
import net.snowflake.client.jdbc.{SnowflakeResultSet, SnowflakeStatement, SnowflakeConnection => CoreSnowflakeConnection}
import reconciliation.QueryStage.{ExecutedQuery, ExecutionQuery}
import exception.PineconeExceptionHandler.exceptionStop
import reconciliation.{QueryRecord, QueryResult}
import java.sql.ResultSet

case class SnowflakeAsyncQuery(
    snowflakeQueryId: String,
	snowflakeQueryStatus: QueryStatus,
    executionQuery: ExecutionQuery
)

class SnowflakeConnection(override val config: Map[String, String]) extends GeneralConnection(config) {
	protected def getDriver: String =  pineconeConf.databaseSupportedDriver.getOrElse("snowflake",
			throw new Exception(s"Database snowflake not supported by Pinecone yet"))

	def submit(queries: List[ExecutionQuery]): List[SnowflakeAsyncQuery] = {
		val statement = this.connection.createStatement
		for {executionQuery <- queries} yield {
			val queryId = statement.unwrap(classOf[SnowflakeStatement]).executeAsyncQuery(executionQuery.query)
				.unwrap(classOf[SnowflakeResultSet]).getQueryID
			SnowflakeAsyncQuery(queryId, QueryStatus.RUNNING, executionQuery)
		}
	}

	def fetch(asyncQueries: List[SnowflakeAsyncQuery]): List[ExecutedQuery] = {
		def queryStatusRecursive(queryStatus: QueryStatus): Unit = {
			Thread.sleep(2000)
			if(queryStatus == QueryStatus.RUNNING) queryStatusRecursive(queryStatus)
		}
		for {asyncQuery <- asyncQueries} yield {
			val resultSet = this.connection.unwrap(classOf[CoreSnowflakeConnection]).createResultSet(asyncQuery.snowflakeQueryId)
			val queryStatus = resultSet.unwrap(classOf[SnowflakeResultSet]).getStatus
			queryStatusRecursive(queryStatus)

			queryStatus match {
				case QueryStatus.SUCCESS => ExecutedQuery(
					asyncQuery.executionQuery.queryKey, resultSetToQueryResult(resultSet), asyncQuery.executionQuery.isTarget
				)
				case _ => exceptionStop(queryStatus.getErrorMessage)
			}
		}
	}

	private def resultSetToQueryResult(rs: ResultSet) : List[QueryResult] = {
		val metadata = rs.getMetaData
		Iterator.from(0).takeWhile(_ => rs.next).map(_ => {
			val records = (for {i <- 1 to metadata.getColumnCount} yield {
				QueryRecord(metadata.getColumnName(i), Some(rs.getObject(i)))
			}).toList
			QueryResult(records)
		}).toList
	}
}
