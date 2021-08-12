package controller

import adapter.GeneralConnection
import anorm.Macro
import anorm.Macro.ColumnNaming.SnakeCase
import com.typesafe.scalalogging.LazyLogging
import configuration.Pinecone.{multiStagesWorkflowsConf, pineconeConf, singleStageWorkflowsConf, stagesConf}
import configuration.{FileBase, MultiStagesWorkflowConf, SQLMethod, SingleStageWorkflowConf, TextBase}

import scala.io.Source


object ApplicationController extends LazyLogging{
	private val databaseConfig = pineconeConf.database
	private implicit val connection: GeneralConnection = GeneralConnection.getJdbcConnection(databaseConfig.connectionName)
	private val fullyQualifiedTablePrefix = s"${databaseConfig.databaseName}.${databaseConfig.schemaName}.PINECONE"

	private val stagesTable = s"${fullyQualifiedTablePrefix}_STAGES"
	private val multiStagesWorkflowTable = s"${fullyQualifiedTablePrefix}_MULTI_STAGES_WORKFLOW"
	private val singleStageWorkflowTable = s"${fullyQualifiedTablePrefix}_SINGLE_STAGE_WORKFLOW"
	private val resultsTable = s"${fullyQualifiedTablePrefix}_RESULTS"

	private val selectQuery = (tableName: String) => s"SELECT * FROM ${tableName}"
	private val stagesParser = Macro.namedParser[Stage](SnakeCase)
	private val singleStageWorkflowParser = Macro.namedParser[SingleStageWorkflowConf](SnakeCase)
	private val multiStagesWorkflowParer = Macro.namedParser[MultiStagesWorkflowConf](SnakeCase)

	private def getStages = {
		stagesConf().stages.map(s => {
			Stage(s.stageKey, getTextFromSQLMethod(s.query), s.isOriginal)
		})
	}

	def init(): Unit = {
		Initialize.createTables(fullyQualifiedTablePrefix)
		val stages = getStages
		Deployer.insert(stagesTable, stages, Macro.toParameters[Stage])
		Deployer.insert(singleStageWorkflowTable, singleStageWorkflowsConf().workflows, Macro.toParameters[SingleStageWorkflowConf])
		Deployer.insert(multiStagesWorkflowTable, multiStagesWorkflowsConf().workflows, Macro.toParameters[MultiStagesWorkflowConf])
	}

	def update(): Unit = {
		val (insertListS, updateListS) = Deployer.categorizeList(getStages, Loader.load(selectQuery(stagesTable), stagesParser))
		if(insertListS.nonEmpty) Deployer.insert(stagesTable, insertListS, Macro.toParameters[Stage])
		if(updateListS.nonEmpty) Deployer.update(stagesTable, updateListS, Macro.toParameters[Stage], Set("STAGE_KEY"))

		val (insertListSW, updateListSW) = Deployer.categorizeList(singleStageWorkflowsConf().workflows, Loader.load(selectQuery(singleStageWorkflowTable), singleStageWorkflowParser))
		if(insertListSW.nonEmpty) Deployer.insert(singleStageWorkflowTable, insertListSW, Macro.toParameters[SingleStageWorkflowConf])
		if(updateListSW.nonEmpty) Deployer.update(singleStageWorkflowTable, updateListSW, Macro.toParameters[SingleStageWorkflowConf], Set("WORKFLOW_KEY"))

		val (insertListMW, updateListMW) = Deployer.categorizeList(multiStagesWorkflowsConf().workflows, Loader.load(selectQuery(multiStagesWorkflowTable), multiStagesWorkflowParer))
		if(insertListSW.nonEmpty) Deployer.insert(multiStagesWorkflowTable, insertListMW, Macro.toParameters[MultiStagesWorkflowConf])
		if(updateListMW.nonEmpty) Deployer.update(multiStagesWorkflowTable, updateListMW, Macro.toParameters[MultiStagesWorkflowConf], Set("WORKFLOW_KEY"))
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
