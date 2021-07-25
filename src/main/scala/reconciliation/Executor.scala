package reconciliation

import adapter.{GeneralConnection, ReconController}
import configuration.Pinecone.pineconeConf
import reconciliation.QueryStage.{ExecutedQuery, ExecutionQuery, PrepareQuery}

import java.util.concurrent.Executors
import scala.concurrent._
import exception.PineconeExceptionHandler.exceptionStop


object Executor {
    private val pickStrategy = pineconeConf.reconciliation.pickStrategy
    private implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))


    def execute = {
        val prepareQueries = ReconController.getQueries

        implicit val executionQueries = Planner.pickPrepareStrategy(pickStrategy, prepareQueries)
        val groupPrepareQueriesByKey = prepareQueries.groupBy(_.queryKey)
        pairExecution(groupPrepareQueriesByKey)
    }

    private def getFutureFromExecutionQuery(query: ExecutionQuery): Future[ExecutedQuery] = {
        val connection = GeneralConnection.getJdbcConnection(query.connectionName)
        Future {connection.executePineconeQuery(query)}
    }

    def pairExecution(groupedQueries: Map[String, List[PrepareQuery]])(implicit queries: List[List[ExecutionQuery]]): Unit = {
        val futures = for(query <- queries) yield {
            val firstQuery = query.head
            val lastQuery = query.last

            if(firstQuery.connectionName == lastQuery.connectionName && GeneralConnection.getDatabaseType(firstQuery.connectionName) == "snowflake") {
                val connection = GeneralConnection.getSnowflakeConnection(firstQuery.connectionName)
                val submitted = connection.submit(query)
                val firstFuture = Future {connection.fetch(submitted.head)}
                val lastFuture = Future {connection.fetch(submitted.last)}
                firstFuture.zip(lastFuture)
            } else {
                val firstFuture = getFutureFromExecutionQuery(firstQuery)
                val lastFuture = getFutureFromExecutionQuery(lastQuery)
                firstFuture.zip(lastFuture)
            }
        }

        val seq = Future.sequence(futures)
        seq.map { sq => {
            sq.map{
                case (f, l) =>
                    val prepareQuery = groupedQueries(f.queryKey)
                    val (sourceQuery, targetQuery) = if (f.isTarget) (l, f) else (f, l)
                    QueryReconciliation.reconcile(sourceQuery, targetQuery, prepareQuery.head)
            }
        }}
    }
}
