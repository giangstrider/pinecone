package adapter

import reconciliation.{QueryColumn, QueryMetadataColumn, QueryRecord}
import reconciliation.QueryStage.{ExecutedQuery, ExecutionQuery}

import java.sql.{Connection, ResultSet, ResultSetMetaData}


trait PineconeExecutionUtility {
	def getResultSet(query: String)(implicit connection : Connection): ResultSet = {
		val statement = connection.createStatement
		statement.executeQuery(query)
	}

	def getQueryResult(resultSet: ResultSet, executionQuery: ExecutionQuery): ExecutedQuery = {
		val metadata = resultSet.getMetaData
		val result = Iterator.from(0).takeWhile(_ => resultSet.next).map(_ => {
			val columns = (for {i <- 1 to metadata.getColumnCount} yield {
				val result = resultSet.getObject(i)
				QueryColumn(metadata.getColumnName(i),
					Some(result),
					getQueryMetadata(metadata, i, result.getClass)
				)
			}).toList
			val key = columns.filter(c => executionQuery.reconcileKey.contains(c.columnName)).mkString("|")
			QueryRecord(columns, key)
		}).toList

		val queryResult = if(result.nonEmpty) Some(result) else None
		ExecutedQuery(executionQuery.queryKey, queryResult, executionQuery.isTarget)
	}

	def getQueryMetadata(metadata: ResultSetMetaData, index: Int, clazz: Class[_]): QueryMetadataColumn = {
		QueryMetadataColumn(
			clazz,
			metadata.getColumnDisplaySize(index),
			metadata.getPrecision(index),
			metadata.getScale(index),
			metadata.isCaseSensitive(index),
			metadata.isCurrency(index),
			metadata.isNullable(index) match {
				case 0 => false
				case _ => true
			}
		)
	}
}