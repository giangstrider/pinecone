package controller

import adapter.GeneralConnection
import anorm.ToParameterList
import com.typesafe.scalalogging.LazyLogging
import configuration.MultiStagesWorkflowConf
import exception.PineconeExceptionHandler.sqlException


object Deployer extends LazyLogging{
	def insert[SW](tableName: String, data: List[SW], macroParam: ToParameterList[SW])(implicit connection: GeneralConnection): Unit = {
		sqlException(connection.executeInsert(tableName, macroParam, data), s"Deploy $tableName successfully", logger)
	}

	def update[SW](tableName: String, data: List[SW], macroParam: ToParameterList[SW], keys: Set[String])(implicit connection: GeneralConnection): Unit = {
		sqlException(connection.executeUpdateById(tableName, macroParam, data, keys), s"Update $tableName successfully", logger)
	}

	private def compareByKey[SW](config: SW, remote: SW): Boolean = {
		(config, remote) match {
			case (a: Stage ,b: Stage) => a.stageKey == b.stageKey
			case (a: MultiStagesWorkflowConf ,b: MultiStagesWorkflowConf) => a.workflowKey == b.workflowKey
		}
	}

	 def categorizeList[SW](config: List[SW], remote: List[SW]): (List[SW], List[SW]) = {
		val diffFromConfigToRemote: List[SW] = config.filterNot(remote.contains)
		val newKeys = diffFromConfigToRemote.filter(
			queryDiff => {! config.exists {queryConnection => compareByKey(queryDiff, queryConnection)}}
		)
		val updatingKeys = diffFromConfigToRemote.filterNot(newKeys.contains)
		(newKeys, updatingKeys)
	}
}
