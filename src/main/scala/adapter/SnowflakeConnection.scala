package adapter

import configuration.Pinecone.pineconeConf
import net.snowflake.client.core.QueryStatus
import net.snowflake.client.jdbc.{SnowflakeResultSet, SnowflakeStatement, SnowflakeConnection => CoreSnowflakeConnection}
import reconciliation.QueryStage.{ExecutedQuery, ExecutionQuery}
import exception.PineconeExceptionHandler.exceptionStop
import scala.annotation.tailrec

case class SnowflakeAsyncQuery(
    snowflakeQueryId: String,
	snowflakeQueryStatus: QueryStatus,
    executionQuery: ExecutionQuery
)


class SnowflakeConnection(override val config: Map[String, String]) extends GeneralConnection(config) with PineconeExecutionUtility {
	protected def getDriver: String =  pineconeConf.databaseSupportedDriver.getOrElse("snowflake",
			throw new Exception(s"Database snowflake not supported by Pinecone yet"))

	def getPineconeExecution(queries: List[ExecutionQuery]): List[ExecutedQuery] = fetch(submit(queries))

	def submit(queries: List[ExecutionQuery]): List[SnowflakeAsyncQuery] = {
		val statement = this.connection.createStatement
		for {executionQuery <- queries} yield {
			val queryId = statement.unwrap(classOf[SnowflakeStatement]).executeAsyncQuery(executionQuery.query)
				.unwrap(classOf[SnowflakeResultSet]).getQueryID
			SnowflakeAsyncQuery(queryId, QueryStatus.RUNNING, executionQuery)
		}
	}

	def fetch(asyncQueries: List[SnowflakeAsyncQuery]): List[ExecutedQuery] = {
		@tailrec def queryStatusRecursive(queryStatus: QueryStatus): Unit = {
			Thread.sleep(2000)
			if(queryStatus == QueryStatus.RUNNING) queryStatusRecursive(queryStatus)
		}
		for {asyncQuery <- asyncQueries} yield {
			val resultSet = this.connection.unwrap(classOf[CoreSnowflakeConnection]).createResultSet(asyncQuery.snowflakeQueryId)
			val queryStatus = resultSet.unwrap(classOf[SnowflakeResultSet]).getStatus
			queryStatusRecursive(queryStatus)

			queryStatus match {
				case QueryStatus.SUCCESS => getQueryResult(resultSet, asyncQuery.executionQuery)
				case _ => exceptionStop(queryStatus.getErrorMessage)
			}
		}
	}
}
