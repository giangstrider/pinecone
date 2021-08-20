package controller

import adapter.GeneralConnection
import anorm.Macro
import anorm.Macro.ColumnNaming.SnakeCase
import com.typesafe.scalalogging.LazyLogging
import configuration.Pinecone.{multiStagesWorkflowsConf, pineconeConf, stagesConf}
import configuration.{FileBase, MultiStagesWorkflowConf, SQLMethod, TextBase}
import reconciliation.QueryStage.{PrepareQuery, ReconciliationQuery}
import reconciliation.ReconcileTypedColumn.{NumberColumn, StringLikeColumn}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.io.Source

case class Stage(
	stageKey: String,
	connectionName: String,
	query: String,
	isOriginal: Boolean
)

case class Result(
     workflowKey: String,
     retryNextRun: Boolean,
     isReconcileKey: Boolean,
     sourceName: String,
     targetName: String,
     sourceMetricName: String,
     targetMetricName: String,
     sourceMetricValue: String,
     targetMetricValue: String,
     variance: String,
     deviation: Double,
     isViolateConfig: Boolean,
     sourceQuery: String,
     targetQuery: String
)


object ApplicationController extends LazyLogging{
	private val executors = Executors.newFixedThreadPool(pineconeConf.concurrency.fixedPoolSize)
    private implicit val executionContext = ExecutionContext.fromExecutorService(executors)
	private val databaseConfig = pineconeConf.database
	private implicit val connection: GeneralConnection = GeneralConnection.getSnowflakeConnection(databaseConfig.connectionName)
	private val fullyQualifiedTablePrefix = s"${databaseConfig.databaseName}.${databaseConfig.schemaName}.PINECONE"

	private val stagesTable = s"${fullyQualifiedTablePrefix}_STAGES"
	private val multiStagesWorkflowTable = s"${fullyQualifiedTablePrefix}_MULTI_STAGES_WORKFLOW"
	private val resultsTable = s"${fullyQualifiedTablePrefix}_RESULTS"

	private val selectQuery = (tableName: String) => s"SELECT * FROM ${tableName}"
	private val stagesParser = Macro.namedParser[Stage](SnakeCase)
	private val multiStagesWorkflowParser = Macro.namedParser[MultiStagesWorkflowConf](SnakeCase)
	private val resultsParser = Macro.namedParser[Result](SnakeCase)

	private def getStages = {
		stagesConf().stages.map(s => {
			Stage(s.stageKey, s.connectionName, getTextFromSQLMethod(s.query), s.isOriginal)
		})
	}

	def init(): Unit = {
		Initialize.createTables(fullyQualifiedTablePrefix)
		val stages = getStages
		Deployer.insert(stagesTable, stages, Macro.toParameters[Stage])
		Deployer.insert(multiStagesWorkflowTable, multiStagesWorkflowsConf().workflows, Macro.toParameters[MultiStagesWorkflowConf])
	}

	def update(): Unit = {
		val (insertListS, updateListS) = Deployer.categorizeList(getStages, Loader.load(selectQuery(stagesTable), stagesParser))
		if(insertListS.nonEmpty) Deployer.insert(stagesTable, insertListS, Macro.toParameters[Stage])
		if(updateListS.nonEmpty) Deployer.update(stagesTable, updateListS, Macro.toParameters[Stage], Set("STAGE_KEY"))

		val (insertListMW, updateListMW) = Deployer.categorizeList(multiStagesWorkflowsConf().workflows, Loader.load(selectQuery(multiStagesWorkflowTable), multiStagesWorkflowParser))
		if(insertListMW.nonEmpty) Deployer.insert(multiStagesWorkflowTable, insertListMW, Macro.toParameters[MultiStagesWorkflowConf])
		if(updateListMW.nonEmpty) Deployer.update(multiStagesWorkflowTable, updateListMW, Macro.toParameters[MultiStagesWorkflowConf], Set("WORKFLOW_KEY"))
	}

	def getQueries(workflows: List[MultiStagesWorkflowConf]): List[PrepareQuery] = {
		val stages = Loader.load(selectQuery(stagesTable), stagesParser).groupBy(_.stageKey)
		val queries = Loader.transformMultiStages(workflows, stages)

		val results = loadRetryResults
		val groupedWorkflows = workflows.groupBy(_.workflowKey)
		val retryQueries = if(results.nonEmpty) {
			val retryWorkflows = results.map(r => groupedWorkflows(r.workflowKey).head)
			val rawRetryQueries = Loader.transformMultiStages(retryWorkflows, stages)
			val groupedResults = results.groupBy(_.workflowKey)
			rawRetryQueries.map(rq => {
				val result = groupedResults(rq.workflowKey).head
				rq.copy(sourceQuery = result.sourceQuery, targetQuery = result.targetQuery)
			})
		} else None

		queries ++ retryQueries
	}

	def getWorkflows: List[MultiStagesWorkflowConf] = Loader.load(selectQuery(multiStagesWorkflowTable), multiStagesWorkflowParser)

	def loadRetryResults: List[Result] = {
		val query = s"${selectQuery(resultsTable)} WHERE RETRY_NEXT_RUN = TRUE AND IS_RECONCILE_KEY = TRUE"
		Loader.load(query, resultsParser)
	}

	def insertResults(results: Future[List[ReconciliationQuery]]): Unit = {
		results.transform {
			case Success(resultsSuccess) => val transformedData = resultsSuccess.flatMap(q => {
			q.records match {
				case Some(records) =>
					records.flatMap(r => { r.reconciled.map(c => {
						val (sourceValue, targetValue, variance, deviation, isViolate) = c.value match {
							case NumberColumn(source, target, different, deviationInside, isMetAcceptedDeviation, isMatched) => (source, target, different, deviationInside, !isMatched)
							case StringLikeColumn(source, target, isMatched) => (source, target, "NOT_APPLY", if(isMatched)  0.0 else 100.0, !isMatched)
						}
						Result(
							q.query.workflowKey, isViolate, c.isReconcileKey, q.query.sourceName, q.query.targetName,
							c.sourceColumnName, c.targetColumnName, sourceValue.getOrElse().toString, targetValue.getOrElse().toString, variance.toString, deviation, isViolate,
							q.query.sourceQuery, q.query.targetQuery
						)
					})
					})
			}
		})
		Try(Deployer.insert(resultsTable, transformedData, Macro.toParameters[Result]))
			case Failure(exception) => Try(println(exception.getMessage))
		}

	}

	private def getTextFromSQLMethod(sqlMethod: SQLMethod): String = {
		sqlMethod match {
			case TextBase(sqlQuery) => sqlQuery
			case FileBase(sqlFile) =>
				val fileSource = Source.fromFile(sqlFile)
				val sqlQuery = fileSource.getLines.mkString(" ")
				fileSource.close
				sqlQuery
		}
	}
}
