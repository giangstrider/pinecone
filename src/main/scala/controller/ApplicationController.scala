package controller

import adapter.GeneralConnection
import anorm.Macro
import anorm.Macro.ColumnNaming.SnakeCase
import com.typesafe.scalalogging.LazyLogging
import configuration.Pinecone.{multiStagesWorkflowsConf, pineconeConf, stagesConf}
import configuration.{FileBase, MultiStagesWorkflowConf, SQLMethod, TextBase}

import scala.io.Source


object ApplicationController extends LazyLogging{
	private val databaseConfig = pineconeConf.database
	private implicit val connection: GeneralConnection = GeneralConnection.getSnowflakeConnection(databaseConfig.connectionName)
	private val fullyQualifiedTablePrefix = s"${databaseConfig.databaseName}.${databaseConfig.schemaName}.PINECONE"

	private val stagesTable = s"${fullyQualifiedTablePrefix}_STAGES"
	private val multiStagesWorkflowTable = s"${fullyQualifiedTablePrefix}_MULTI_STAGES_WORKFLOW"
	private val resultsTable = s"${fullyQualifiedTablePrefix}_RESULTS"

	private val selectQuery = (tableName: String) => s"SELECT * FROM ${tableName}"
	private val stagesParser = Macro.namedParser[Stage](SnakeCase)
	private val multiStagesWorkflowParer = Macro.namedParser[MultiStagesWorkflowConf](SnakeCase)

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

		val (insertListMW, updateListMW) = Deployer.categorizeList(multiStagesWorkflowsConf().workflows, Loader.load(selectQuery(multiStagesWorkflowTable), multiStagesWorkflowParer))
		if(insertListMW.nonEmpty) Deployer.insert(multiStagesWorkflowTable, insertListMW, Macro.toParameters[MultiStagesWorkflowConf])
		if(updateListMW.nonEmpty) Deployer.update(multiStagesWorkflowTable, updateListMW, Macro.toParameters[MultiStagesWorkflowConf], Set("WORKFLOW_KEY"))
	}

	def getQueries = {
		val stages = Loader.load(selectQuery(stagesTable), stagesParser)
		val multiStagesWorkflows = Loader.load(selectQuery(multiStagesWorkflowTable), multiStagesWorkflowParer)
		Loader.transformMultiStages(multiStagesWorkflows, stages)
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
