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
			  | CAN_EMPTY BOOLEAN DEFAULT FALSE,
			  | CAN_EMPTY_ON_CONSECUTIVE_TIMES BOOLEAN DEFAULT FALSE,
			  | MAXIMUM_EMPTY_ON_CONSECUTIVE_TIMES NUMBER(10, 0),
			  | INSERT_TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
			  | UPDATE_TIMESTAMP TIMESTAMP_TZ,
			  | CONSTRAINT PK_PINECONE_MULTI_STAGES_WORKFLOW PRIMARY KEY (WORKFLOW_KEY)
			  |)""".stripMargin

		val resultsTableSql =
			s"""
			  |CREATE ${replaceSql} TABLE ${qualifiedSchemaName}_RESULTS(
			  | RESULT_ID NUMBER(38,0) NOT NULL AUTOINCREMENT,
			  | WORKFLOW_KEY VARCHAR(256) NOT NULL,
			  | RETRY_NEXT_RUN BOOLEAN DEFAULT FALSE,
			  | IS_RECONCILE_KEY BOOLEAN,
			  | SOURCE_NAME VARCHAR(256) NOT NULL,
			  | TARGET_NAME VARCHAR(256),
			  | SOURCE_METRIC_NAME VARCHAR(256),
			  | TARGET_METRIC_NAME VARCHAR(256),
			  | SOURCE_METRIC_VALUE TEXT,
			  | TARGET_METRIC_VALUE TEXT,
			  | VARIANCE TEXT,
			  | DEVIATION NUMBER(23, 5),
			  | IS_VIOLATE_CONFIG BOOLEAN,
			  | SOURCE_QUERY TEXT,
			  | TARGET_QUERY TEXT,
			  | INSERT_TIMESTAMP TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
			  | UPDATE_TIMESTAMP TIMESTAMP_TZ,
			  | CONSTRAINT PK_PINECONE_RESULTS PRIMARY KEY (RESULT_ID)
			  |)""".stripMargin

		List(stagesTableSql, multiStagesWorkflowTableSql, resultsTableSql).foreach(q => {
			sqlException(connection.executeUpdate(q), s"DDL ${q} execute successfully", logger)
		})
	}
}
