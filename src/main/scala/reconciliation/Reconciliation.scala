package reconciliation

import configuration.Pinecone.pineconeConf
import reconciliation.QueryStage.{ExecutedQuery, PrepareQuery, ReconciliationRecord}
import reconciliation.ReconcileTypedColumn.{NumberColumn, StringLikeColumn}
import exception.PineconeExceptionHandler.exceptionStop

import java.util.Date
import java.sql.Timestamp


object Reconciliation {
	private def processMissingPair(records: List[QueryRecord], isTarget: Boolean, prepareQuery: PrepareQuery): List[ReconciliationRecord] = {
		records.map(queryRecord => {
			val attributes = queryRecord.columns.filterNot(c => prepareQuery.reconcileKey.contains(c.columnName))
			val reconciliation = if (isTarget)
				attributes.map(c => convertToReconcileColumn(c.columnName, None, c.columnValue))
			else
				attributes.map(c => convertToReconcileColumn(c.columnName, c.columnValue, None))
			val reconcileKeyColumns = queryRecord.columns.filter(c => prepareQuery.reconcileKey.contains(c.columnName))
				.map(qc => ReconcileKeyColumn(qc.columnName, qc.columnValue.get.toString))
				ReconciliationRecord(prepareQuery.queryKey, reconcileKeyColumns, reconciliation)
		})
	}

	def reconcile(source: ExecutedQuery, target: ExecutedQuery, prepareQuery: PrepareQuery): List[ReconciliationRecord] = {
		val groupRecords = (source.result ++ target.result).groupBy(_.reconcileKeyValue)

		val matchedPair = groupRecords.filter(_._2.size == 2).values.map {pair =>
			val sourceColumns = pair.head.columns
			val targetColumns = pair.last.columns

			val filterAndSort = (v: List[QueryColumn]) => v.filterNot(vc => prepareQuery.reconcileKey.contains(vc.columnName)).sortBy(_.columnName)
			val attributeSource = filterAndSort(sourceColumns)
			val attributeTarget = filterAndSort(targetColumns)

			val reconcileKeyColumns = sourceColumns.filter(c => prepareQuery.reconcileKey.contains(c.columnName))
					.map(qc => ReconcileKeyColumn(qc.columnName, qc.columnValue.get.toString))

			val pairAttributes = attributeSource.map {sc =>
				attributeTarget.find(c => sc.columnName == c.columnName) match {
					case Some(t) => convertToReconcileColumn(sc.columnName, sc.columnValue, t.columnValue)
					case None => convertToReconcileColumn(sc.columnName, sc.columnValue, None)
				}
			}

			val noneSourceAttributes = attributeTarget.filterNot(t => attributeSource.map(_.columnName)
				.contains(t.columnName)).map(t => convertToReconcileColumn(t.columnName, None, t.columnValue))

			ReconciliationRecord(prepareQuery.queryKey, reconcileKeyColumns, pairAttributes ++ noneSourceAttributes)
		}.toList

		val missingPairKey = groupRecords.filter(_._2.size == 1).keySet
		if(missingPairKey.nonEmpty) {
			val filterReconcileKey = (v: ExecutedQuery) => v.result.filter(e => missingPairKey.contains(e.reconcileKeyValue))
			val filteredSource = filterReconcileKey(source)
			val filteredTarget = filterReconcileKey(target)
			val combinedRecords = (filteredSource, filteredTarget) match {
				case v@(_, _) if v._1.nonEmpty && v._2.nonEmpty =>
					processMissingPair(v._1, isTarget = false, prepareQuery) ++ processMissingPair(v._2, isTarget = true, prepareQuery)
				case v@(_, _) if v._1.nonEmpty && v._2.isEmpty => processMissingPair(v._1, isTarget = false, prepareQuery)
				case v@(_, _) if v._1.isEmpty && v._2.nonEmpty => processMissingPair(v._2, isTarget = true, prepareQuery)
			}
			return matchedPair ++ combinedRecords
		}

		matchedPair
	}

	private def convertToReconcileColumn(columnName: String, source: Option[Any], target: Option[Any]): ReconcileTypedColumn = {
		(source, target) match {
			case (Some(sv), Some(tv)) => (sv, tv) match {
				case v@((_: Short, _: Short) | (_: Int, _: Int) | (_: Long, _: Long) | (_: Double, _: Double)) =>
					numericReconcile(columnName, Some(v._1.asInstanceOf[Number].doubleValue), Some(v
						._2.asInstanceOf[Number].doubleValue))

				case v@((_: BigInt, _: BigInt) | (_: BigDecimal, _: BigDecimal)) =>
					numericReconcile(columnName, Some(v._1.asInstanceOf[BigDecimal]), Some(v._2.asInstanceOf[BigDecimal]))
				case v@((_: Date, _: Date) | (_: Timestamp, _: Timestamp) | (_: String, _: String)) =>
					stringLikeReconcile(columnName, Some(v._1), Some(v._2))
			}
			case (Some(sv), None) => sv match {
				case s@(_: Short | _: Int | _: Long | _: Double) => numericReconcile(columnName, Some(s
					.asInstanceOf[Number].doubleValue), None)
				case s@(_: BigInt | _: BigDecimal) => numericReconcile(columnName, Some(s.asInstanceOf[BigDecimal]), None)
				case s@(_: Date | _: Timestamp | _: String) => stringLikeReconcile(columnName, Some(s), None)
			}
			case (None, Some(tv)) => tv match {
				case t@(_: Short | _: Int | _: Long | _: Double) => numericReconcile(columnName, None, Some(t
					.asInstanceOf[Number].doubleValue))
				case t@(_: BigInt | _: BigDecimal) => numericReconcile(columnName, None, Some(t.asInstanceOf[BigDecimal]))
				case t@(_: Date | _: Timestamp | _: String) =>
					stringLikeReconcile(columnName, None, Some(t))
			}
			case (None, None) => exceptionStop("Source and Target cannot be both Nne")
		}
	}

	private def numericReconcile[N](columnName: String, source: Option[N], target: Option[N])(implicit n: Fractional[N])
	: NumberColumn[N] = {
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

	private def stringLikeReconcile[S](columnName: String, source: Option[S], target: Option[S]): StringLikeColumn[S]
	= {
		val isMatched: Boolean = if(source == target) true else false
		StringLikeColumn(columnName,source, target, isMatched)
	}

	private def setScale(number: Double): Double = {
		val decimalScale = pineconeConf.reconciliation.decimalScale
		BigDecimal(number).setScale(decimalScale, BigDecimal.RoundingMode.HALF_EVEN).doubleValue
	}

	def validateQueryColumn = ???
	//Reject column name appear more than one
}
