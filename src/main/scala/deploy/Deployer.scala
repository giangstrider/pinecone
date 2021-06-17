package deploy

import configuration.Pinecone.queriesConf
import adapter.ReconController
import configuration.{FileBase, QueryConf, SQLMethod, TextBase}
import reconciliation.QueryStage.PrepareQuery
import scala.io.Source


object Deployer {
	val queriesFromConnection: List[PrepareQuery] = ReconController.getQueries
	val queriesFromConfig: List[PrepareQuery] = queriesConf.queries.map(convertConfToPrepareQuery)
	val queriesConfigDiff: List[PrepareQuery] =  queriesFromConfig.filterNot(queriesFromConnection.contains)

	def deploy = {
		val (insertList, updateList) = categorizeList
		if(insertList.nonEmpty) ReconController.insert(insertList)
		if(updateList.nonEmpty) ReconController.update(updateList)
	}

	private def categorizeList: (List[PrepareQuery], List[PrepareQuery]) = {
		val newKey = queriesConfigDiff.filter(
			queryDiff => {! queriesFromConnection.exists {queryConnection => compareByKey(queryDiff, queryConnection)}}
		)
		(newKey, queriesConfigDiff.filterNot(newKey.contains))
	}

	private def compareByKey(config: PrepareQuery, connection: PrepareQuery): Boolean = {
		config.queryKey == connection.queryKey &&
		config.sourceName == connection.sourceName &&
		config.targetName == connection.targetName
	}

	private def convertConfToPrepareQuery(query: QueryConf): PrepareQuery = {
		PrepareQuery(
			query.queryKey,
			query.sourceName, getTextFromSQLMethod(query.sourceQuery),
			query.targetName, getTextFromSQLMethod(query.targetQuery),
			query.acceptedDeviation
		)
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
