package reconciliation

import reconciliation.QueryStage.{ExecutedQuery, PrepareQuery, ReconciliationRecord}
import exception.PineconeExceptionHandler.exceptionStop


object Reconciliation {
	private val scaleRoundUp = 10

	private def processMissingPair(pairKeys: Set[String], executedQuery: ExecutedQuery, prepareQuery: PrepareQuery)
	: Option[List[ReconciliationRecord]] = {
		Some(
			executedQuery.result.filter(e => pairKeys.contains(e.reconcileKeyValue)).map(queryRecord => {
				val attributes = queryRecord.columns.filterNot(c => prepareQuery.reconcileKey.contains(c.columnName))
				val reconciliation = if (executedQuery.isTarget) {
					attributes.map(c => ReconcileColumn(c.columnName, Some(0), c.columnValue, -convertDouble(c.columnValue.get), -100))
				} else {
					attributes.map(c => ReconcileColumn(c.columnName, c.columnValue, Some(0), convertDouble(c.columnValue.get), 100))
				}
				ReconciliationRecord(prepareQuery.queryKey, queryRecord.reconcileKeyValue, reconciliation)
			})
		)
	}

	def reconcile(source: ExecutedQuery, target: ExecutedQuery, prepareQuery: PrepareQuery): List[ReconciliationRecord] = {
		val groupRecords = (source.result ++ target.result).groupBy(_.reconcileKeyValue)
		val missingPairKey = groupRecords.filter(_._2.size == 1).keySet
		val missedSourceRecords = processMissingPair(missingPairKey, source, prepareQuery)
		val missedTargetRecords = processMissingPair(missingPairKey, target, prepareQuery)

		val matchedPair = groupRecords.filter(_._2.size == 2).values.map {pair =>
			val sourceColumn = pair.head.columns
			val targetColumn = pair.last.columns

			if(sourceColumn.size != targetColumn.size) println("WARNING")

			val attributeSource = sourceColumn.filterNot(c => prepareQuery.reconcileKey.contains(c.columnName))
			val attributeTarget = targetColumn.filterNot(c => prepareQuery.reconcileKey.contains(c.columnName))
			val reconciliation = attributeSource.map {sc =>
				val tc = attributeTarget.filter(c => sc.columnName == c.columnName).head
				val sourceValue = convertDouble(sc.columnValue.get)
				val targetValue = convertDouble(tc.columnValue.get)
				val different: Double = sourceValue - targetValue
				val deviation: Double = setScale((different / sourceValue) * 100)
				ReconcileColumn(sc.columnName, Some(sourceValue), Some(targetValue), different, deviation)
			}

			ReconciliationRecord(prepareQuery.queryKey, pair.head.reconcileKeyValue, reconciliation)
		}.toList

		matchedPair ++ missedSourceRecords.get ++ missedTargetRecords.get
	}

	private def convertDouble(anyVal: Any): Double = anyVal match {
		case n: java.lang.Number => n.doubleValue
		case _ =>
			exceptionStop(s"Cannot convert ${anyVal.toString} to Numeric(Double)")
	}

	private def setScale(number: Double): Double =
		BigDecimal(number).setScale(scaleRoundUp, BigDecimal.RoundingMode.HALF_EVEN).doubleValue

	def validateQueryColumn = ???
}
