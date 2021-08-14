package controller

import adapter.GeneralConnection
import com.typesafe.scalalogging.LazyLogging
import exception.PineconeExceptionHandler.sqlException

object Initialize extends LazyLogging{
	def createTables(qualifiedSchemaName: String, replace: Boolean = true)(implicit connection: GeneralConnection): Unit = {
		val replaceSql = if(replace) "OR REPLACE" else ""
		val stagesTableSql =
			s"""
			  |CREATE ${replaceSql} TABLE ${qualifiedSchemaName}_STAGES(
			  | STAGE_KEY VARCHAR(256),
			  | CONNECTION_NAME VARCHAR(256),
			  | QUERY TEXT NOT NULL,
			  | IS_ORIGINAL BOOLEAN DEFAULT FALSE,
			  | INSERT_TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
			  | UPDATE_TIMESTAMP TIMESTAMP_TZ,
			  | CONSTRAINT PK_PINECONE_STAGES PRIMARY KEY (STAGE_KEY)
			  |)""".stripMargin

		val multiStagesWorkflowTableSql = s"""
			  |CREATE ${replaceSql} TABLE ${qualifiedSchemaName}_MULTI_STAGES_WORKFLOW(
			  | WORKFLOW_KEY VARCHAR(256),
			  | STAGES TEXT NOT NULL,
			  | RECONCILE_KEYS TEXT NOT NULL,
			  | ACCEPTED_DEVIATION NUMBER(23,5) DEFAULT 0,
			  | INSERT_TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
			  | UPDATE_TIMESTAMP TIMESTAMP_TZ,
			  | CONSTRAINT PK_PINECONE_MULTI_STAGES_WORKFLOW PRIMARY KEY (WORKFLOW_KEY)
			  |)""".stripMargin

		val singleStagesWorkflowTableSql = s"""
			  |CREATE ${replaceSql} TABLE ${qualifiedSchemaName}_SINGLE_STAGE_WORKFLOW(
			  | WORKFLOW_KEY VARCHAR(256),
			  | STAGE VARCHAR(256) NOT NULL,
			  | CAN_EMPTY BOOLEAN DEFAULT FALSE,
			  | CAN_EMPTY_ON_CONSECUTIVE_TIMES BOOLEAN DEFAULT FALSE,
			  | MAXIMUM_EMPTY_ON_CONSECUTIVE_TIMES NUMBER(10, 0),
			  | INSERT_TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
			  | UPDATE_TIMESTAMP TIMESTAMP_TZ,
			  | CONSTRAINT PK_PINECONE_SINGLE_STAGE_WORKFLOW PRIMARY KEY (WORKFLOW_KEY)
			  |)""".stripMargin

		val resultsTableSql =
			s"""
			  |CREATE ${replaceSql} TABLE ${qualifiedSchemaName}_RESULTS(
			  | RESULT_ID NUMBER(38,0) NOT NULL AUTOINCREMENT,
			  | REPORT_ID VARCHAR(256) UNIQUE NOT NULL,
			  | REPORT_TIMESTAMP TIMESTAMP_TZ NOT NULL,
			  | WORKFLOW_KEY VARCHAR(256) NOT NULL,
			  | WORKFLOW_TYPE VARCHAR(12) NOT NULL,
			  | RETRY_NEXT_RUN BOOLEAN DEFAULT FALSE,
			  | RETRY_PARAM TEXT,
			  | RETRIED_TIMES NUMBER(10, 0) DEFAULT 0,
			  | STAGE_SOURCE_KEY VARCHAR(256) NOT NULL,
			  | STAGE_TARGET_KEY VARCHAR(256),
			  | STAGE_SOURCE_METRIC_NAME VARCHAR(256),
			  | STAGE_SOURCE_METRIC_VALUE TEXT,
			  | STAGE_TARGET_METRIC_NAME  VARCHAR(256),
			  | STAGE_TARGET_METRIC_VALUE TEXT,
			  | VARIANCE TEXT,
			  | DEVIATION NUMBER(23, 5),
			  | INSERT_TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
			  | UPDATE_TIMESTAMP TIMESTAMP_TZ,
			  | CONSTRAINT PK_PINECONE_RESULTS PRIMARY KEY (RESULT_ID)
			  |)""".stripMargin

		List(stagesTableSql, multiStagesWorkflowTableSql, singleStagesWorkflowTableSql, resultsTableSql).foreach(q => {
			sqlException(connection.executeUpdate(q), s"DDL ${q} execute successfully", logger)
		})
	}
}
