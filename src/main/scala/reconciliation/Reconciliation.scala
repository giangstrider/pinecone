package reconciliation

import configuration.Pinecone.pineconeConf
import reconciliation.QueryStage.{ExecutedQuery, PrepareQuery, ReconciliationRecord}
import reconciliation.ReconcileTypedColumn.{NumberColumn, StringLikeColumn}
import exception.PineconeExceptionHandler.exceptionStop

import java.util.Date
import java.sql.Timestamp


object Reconciliation {
	private def processMissingPair(pairKeys: Set[String], executedQuery: ExecutedQuery, prepareQuery: PrepareQuery)
	: Option[List[ReconciliationRecord]] = {
		Some(
			executedQuery.result.filter(e => pairKeys.contains(e.reconcileKeyValue)).map(queryRecord => {
				val attributes = queryRecord.columns.filterNot(c => prepareQuery.reconcileKey.contains(c.columnName))
				val reconciliation = if (executedQuery.isTarget)
					attributes.map(c => convertToReconcileColumn(c.columnName, None, c.columnValue))
					else
					attributes.map(c => convertToReconcileColumn(c.columnName, c.columnValue, None))
				val reconcileKeyColumns = queryRecord.columns.filter(c => prepareQuery.reconcileKey.contains(c.columnName))
					.map(qc => ReconcileKeyColumn(qc.columnName, qc.columnValue.get.toString))
				ReconciliationRecord(prepareQuery.queryKey, reconcileKeyColumns, reconciliation)
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
				convertToReconcileColumn(sc.columnName, sc.columnValue, tc.columnValue)
			}

			val reconcileKeyColumns = sourceColumn.filter(c => prepareQuery.reconcileKey.contains(c.columnName))
					.map(qc => ReconcileKeyColumn(qc.columnName, qc.columnValue.get.toString))

			ReconciliationRecord(prepareQuery.queryKey, reconcileKeyColumns, reconciliation)
		}.toList

		matchedPair ++ missedSourceRecords.get ++ missedTargetRecords.get
	}

	private def convertToReconcileColumn(columnName: String, source: Option[Any], target: Option[Any]): ReconcileTypedColumn = {
		(source, target) match {
			case (Some(sv), Some(tv)) => (sv, tv) match {
				case v@((_: Short, _: Short) | (_: Int, _: Int) | (_: Long, _: Long) | (_: Double, _: Double)) =>
					typedReconcile(columnName, Some(v._1.asInstanceOf[Number].doubleValue), Some(v
						._2.asInstanceOf[Number].doubleValue))

				case v@((_: BigInt, _: BigInt) | (_: BigDecimal, _: BigDecimal)) =>
					typedReconcile(columnName, Some(v._1.asInstanceOf[BigDecimal]), Some(v._2.asInstanceOf[BigDecimal]))
				case v@((_: Date, _: Date) | (_: Timestamp, _: Timestamp) | (_: String, _: String)) =>
					typedReconcile(columnName, Some(v._1.asInstanceOf[String]), Some(v._2.asInstanceOf[String]))
			}
			case (Some(sv), None) => sv match {
				case s@(_: Short | _: Int | _: Long | _: Double) => typedReconcile(columnName, Some(s
					.asInstanceOf[Number].doubleValue), None)
				case s@(_: BigInt | _: BigDecimal) => typedReconcile(columnName, Some(s.asInstanceOf[BigDecimal]), None)
				case s@(_: Date | _: Timestamp | _: String) => typedReconcile(columnName, Some(s.asInstanceOf[BigDecimal]), None)
			}
			case (None, Some(tv)) => tv match {
				case t@(_: Short | _: Int | _: Long | _: Double) => typedReconcile(columnName, None, Some(t
					.asInstanceOf[Number].doubleValue))
				case t@(_: BigInt | _: BigDecimal) => typedReconcile(columnName, None, Some(t.asInstanceOf[BigDecimal]))
				case t@(_: Date | _: Timestamp | _: String) => typedReconcile(columnName, None, Some(t.asInstanceOf[BigDecimal]))
			}
			case (None, None) => exceptionStop("Source and Target cannot be both Nne")
		}
	}

	private def typedReconcile[N](columnName: String, source: Option[N], target: Option[N])(implicit n: Fractional[N]): NumberColumn[N] = {
		import n._
		val valueToN = (value: Option[N]) => value match {
			case Some(v) => v
			case None => fromInt(0)
		}
		val sourceN = valueToN(source)
		val targetN = valueToN(target)
		val different: N = sourceN - targetN
		val deviation: Double = if(equiv(abs(different), targetN)) -100 else setScale(toDouble(different / sourceN) * 100)
		val isMatched: Boolean = equiv(sourceN, targetN)
		NumberColumn(columnName, sourceN, targetN, different, deviation, isMatched)
	}

	private def typedReconcile(columnName: String, source: Option[String], target: Option[String]): StringLikeColumn = {
		val isMatched: Boolean = if(source == target) true else false
		StringLikeColumn(columnName,source, target, isMatched)
	}

	private def setScale(number: Double): Double = {
		val decimalScale = pineconeConf.reconciliation.decimalScale
		BigDecimal(number).setScale(decimalScale, BigDecimal.RoundingMode.HALF_EVEN).doubleValue
	}

	def validateQueryColumn = ???
}
