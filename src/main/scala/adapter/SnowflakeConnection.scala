package adapter

import configuration.Pinecone.pineconeConf
import net.snowflake.client.core.QueryStatus
import net.snowflake.client.jdbc.{SnowflakeResultSet, SnowflakeStatement, SnowflakeConnection => CoreSnowflakeConnection}
import reconciliation.QueryStage.{ExecutedQuery, ExecutionQuery}
import exception.PineconeExceptionHandler.exceptionStop

import java.sql.ResultSet
import scala.annotation.tailrec

case class SnowflakeAsyncQuery(
    snowflakeQueryId: String,
	snowflakeQueryStatus: QueryStatus,
    executionQuery: ExecutionQuery
)


class SnowflakeConnection(override val config: Map[String, String]) extends GeneralConnection(config) {
	protected def getDriver: String =  pineconeConf.databaseSupportedDriver.getOrElse("snowflake",
			throw new Exception(s"Database snowflake not supported by Pinecone yet"))

	def executeBatchAsync(queries: List[ExecutionQuery]): List[ExecutedQuery] = for (query <- submit(queries)) yield fetch(query)

	def submit(queries: List[ExecutionQuery]): List[SnowflakeAsyncQuery] = {
		val statement = this.connection.createStatement
		for {executionQuery <- queries} yield {
			val queryId = statement.unwrap(classOf[SnowflakeStatement]).executeAsyncQuery(executionQuery.query)
				.unwrap(classOf[SnowflakeResultSet]).getQueryID
			SnowflakeAsyncQuery(queryId, QueryStatus.RUNNING, executionQuery)
		}
	}

	def fetch(asyncQuery: SnowflakeAsyncQuery): ExecutedQuery = {
		@tailrec def queryStatusRecursive(resultSet: ResultSet): QueryStatus = {
			val queryStatus = resultSet.unwrap(classOf[SnowflakeResultSet]).getStatus
			if(queryStatus == QueryStatus.RUNNING || queryStatus == QueryStatus.RESUMING_WAREHOUSE || queryStatus == QueryStatus.NO_DATA) {
			Thread.sleep(2000)
				queryStatusRecursive(resultSet)
			} else queryStatus
		}

			val resultSet = this.connection.unwrap(classOf[CoreSnowflakeConnection]).createResultSet(asyncQuery.snowflakeQueryId)
		val queryStatus = queryStatusRecursive(resultSet)

			queryStatus match {
				case QueryStatus.SUCCESS => getQueryResult(resultSet, asyncQuery.executionQuery)
			case QueryStatus.FAILED_WITH_ERROR => exceptionStop(queryStatus.getErrorMessage)
			case _ => exceptionStop(queryStatus.getDescription)
		}
	}
}
