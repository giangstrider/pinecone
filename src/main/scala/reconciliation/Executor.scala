package reconciliation

import adapter.GeneralConnection
import com.typesafe.scalalogging.Logger
import configuration.Pinecone.pineconeConf
import reconciliation.QueryStage.{ExecutedQuery, ExecutionQuery, PrepareQuery, ReconciliationQuery}

import java.util.concurrent.Executors
import scala.concurrent._
import exception.PineconeExceptionHandler.exceptionStop

import scala.util.{Failure, Success, Try}


object Executor {
    private val pickStrategy = pineconeConf.reconciliation.pickStrategy
    private val executors = Executors.newFixedThreadPool(pineconeConf.concurrency.fixedPoolSize)
    private implicit val executionContext = ExecutionContext.fromExecutorService(executors)


    def execute(prepareQueries: List[PrepareQuery]): Future[List[ReconciliationQuery]] = {
        implicit val executionQueries = Planner.pickPrepareStrategy("pair", prepareQueries)
        val groupPrepareQueriesByKey = prepareQueries.groupBy(_.queryKey)

        pickStrategy match {
			case "pair" => pairExecution(groupPrepareQueriesByKey)
			case "connection" => connectionGroupExecution(groupPrepareQueriesByKey)
			case _ => exceptionStop("No valid strategy found")
		}
    }

    def connectionGroupExecution(groupedQueries: Map[String, List[PrepareQuery]])(implicit queries: List[List[ExecutionQuery]]): Future[List[ReconciliationQuery]] = {
        val futures = for(query <- queries) yield {
            val firstQuery = query.head

            if(GeneralConnection.getDatabaseType(firstQuery.connectionName) == "snowflake") {
                val connection = GeneralConnection.getSnowflakeConnection(firstQuery.connectionName)
                val submittedQueries = connection.submit(query)
                submittedQueries.map(q => Future { blocking {connection.fetch(q)}})
            } else {

                val connection = GeneralConnection.getJdbcConnection(firstQuery.connectionName)
                query.map(q => Future { blocking{ connection.executePineconeQuery(q)}})
            }
        }

         Future.sequence(futures.flatten).transform {
            case Success(vl) =>
                val reconciled = vl.groupBy(_.queryKey).map {
                    case (k, v) =>
                        val (sourceQuery, targetQuery) = classifySource(v.head, v.last)
                        val prepareQuery = groupedQueries(k)
                        QueryReconciliation.reconcile(sourceQuery, targetQuery, prepareQuery.head)
                    case _ => ???
                }.toList
                executors.shutdown()
                Logger("Executor").info("Complete")
                Logger("Executor").info(reconciled.toString())

                Try(reconciled)
            case Failure(exception) => println(exception.getMessage); exceptionStop(exception)
        }
    }

    def pairExecution(groupedQueries: Map[String, List[PrepareQuery]])(implicit queries: List[List[ExecutionQuery]]): Future[List[ReconciliationQuery]] = {
        val futures = for(query <- queries) yield {
            val firstQuery = query.head
            val lastQuery = query.last

            val (firstFuture, secondFuture) = if(firstQuery.connectionName == lastQuery.connectionName && GeneralConnection.getDatabaseType(firstQuery.connectionName) == "snowflake") {
                val connection = GeneralConnection.getSnowflakeConnection(firstQuery.connectionName)
                val submitted = connection.submit(query)
                (Future {connection.fetch(submitted.head)}, Future {connection.fetch(submitted.last)})
            } else {
                val firstConnection = GeneralConnection.getJdbcConnection(firstQuery.connectionName)
                val lastConnection = GeneralConnection.getJdbcConnection(lastQuery.connectionName)
                (Future { blocking {firstConnection.executePineconeQuery(firstQuery)}}, Future { blocking {lastConnection.executePineconeQuery(lastQuery)}})
            }
//            val firstConnection = GeneralConnection.getJdbcConnection(firstQuery.connectionName)
//             val lastConnection = GeneralConnection.getJdbcConnection(lastQuery.connectionName)
//            val (firstFuture, secondFuture) = (Future { blocking {firstConnection.executePineconeQuery(firstQuery)}}, Future { blocking {lastConnection.executePineconeQuery(lastQuery)}})

            firstFuture.zipWith(secondFuture)((f, l) => {
                val prepareQuery = groupedQueries(f.queryKey)
                val (sourceQuery, targetQuery) = classifySource(f, l)
                QueryReconciliation.reconcile(sourceQuery, targetQuery, prepareQuery.head)
            })
        }

        Future.sequence(futures).transform {
            case Success(reconciled) =>
//                executors.shutdown()
                Logger("Executor").info("Complete")
                Logger("Executor").info(reconciled.toString())
                Try(reconciled)
            case Failure(exception) => println("thala vl"); println(exception.printStackTrace()); exceptionStop(exception)
        }
    }

    private def classifySource(v1: ExecutedQuery, v2: ExecutedQuery) = if (v1.isTarget) (v2, v1) else (v1, v2)
}
