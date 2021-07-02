package reconciliation

import configuration.Pinecone.pineconeConf
import reconciliation.QueryStage.{PrepareQuery, ReconciliationRecord}
import reconciliation.ReconcileTypedColumn.{NumberColumn, StringLikeColumn}
import exception.PineconeExceptionHandler.exceptionStop

import java.util.Date
import java.sql.Timestamp


object RecordReconciliation {
	def reconcile(source: Option[List[QueryRecord]], target: Option[List[QueryRecord]], query: PrepareQuery): List[ReconciliationRecord] = {
		val reconcileKeyColumns = (v: List[QueryColumn]) => v.filter(c => query.reconcileKey.contains(c.columnName))
									.map(qc => ReconcileKeyColumn(qc.columnName, qc.columnValue.get.toString))
		val filterAndSort = (v: List[QueryColumn]) => v.filterNot(vc => query.reconcileKey.contains(vc.columnName)).sortBy(_.columnName)

		(source, target) match {
			case (Some(s), Some(t)) =>
				val groupRecords = (s ++ t).groupBy(_.reconcileKeyValue)
				val matchedPair = groupRecords.filter(_._2.size == 2).values.map(pair => {
					val columnsS = pair.head.columns
					val columnsT = pair.last.columns

					val attributeS = filterAndSort(columnsS)
					val attributesT = filterAndSort(columnsT)

					val pairAttributes =  attributeS.map(sc => {
						attributesT.find(c => sc.columnName == c.columnName) match {
							case Some(tc) => convertToReconcileColumn(sc.columnName, sc.columnValue, tc.columnValue)
							case None => convertToReconcileColumn(sc.columnName, sc.columnValue, None)
						}
					})

					val missTargetAttributes = attributesT.filterNot(tm => attributeS.map(_.columnName)
						.contains(tm.columnName)).map(tm => convertToReconcileColumn(tm.columnName, None, tm.columnValue))

					ReconciliationRecord(query.queryKey, reconcileKeyColumns(columnsS), pairAttributes ++ missTargetAttributes)
				}).toList

				val missingPairKey = groupRecords.filter(_._2.size == 1).keySet
				if(missingPairKey.nonEmpty) {
					val filterReconcileKey = (v: List[QueryRecord]) => v.filter(e => missingPairKey.contains(e.reconcileKeyValue))
					val filteredS = filterReconcileKey(s)
					val filteredT = filterReconcileKey(t)
					val combinedRecords = (filteredS, filteredT) match {
						case v@(_, _) if v._1.nonEmpty && v._2.nonEmpty =>
							processMissingPair(v._1, isTarget = false, query) ++ processMissingPair(v._2,true, query)
						case v@(_, _) if v._1.nonEmpty && v._2.isEmpty => processMissingPair(v._1, false, query)
						case v@(_, _) if v._1.isEmpty && v._2.nonEmpty => processMissingPair(v._2, true, query)
					}
					return matchedPair ++ combinedRecords
				}

				matchedPair

			case (Some(v), None) =>	v.map(qc => {
					val columns = filterAndSort(qc.columns)
					val pairAttributes = columns.map(c => {convertToReconcileColumn(c.columnName, c.columnValue, None)})
					ReconciliationRecord(query.queryKey, reconcileKeyColumns(qc.columns), pairAttributes)
				})

			case (None, Some(v)) =>	v.map(qc => {
					val columns = filterAndSort(qc.columns)
					val pairAttributes = columns.map(c => {convertToReconcileColumn(c.columnName, None, c.columnValue)})
					ReconciliationRecord(query.queryKey, reconcileKeyColumns(qc.columns), pairAttributes)
				})
		}
	}

	private def processMissingPair(records: List[QueryRecord], isTarget: Boolean, prepareQuery: PrepareQuery): List[ReconciliationRecord] = {
		records.map(qc => {
			val columns = qc.columns
			val attributes = columns.filterNot(c => prepareQuery.reconcileKey.contains(c.columnName))
			val reconciliation = if (isTarget) attributes.map(c => convertToReconcileColumn(c.columnName, None, c.columnValue))
								else attributes.map(c => convertToReconcileColumn(c.columnName, c.columnValue, None))
			val reconcileKeyColumns = columns.filter(c => prepareQuery.reconcileKey.contains(c.columnName))
								.map(qc => ReconcileKeyColumn(qc.columnName, qc.columnValue.get.toString))
			ReconciliationRecord(prepareQuery.queryKey, reconcileKeyColumns, reconciliation)
		})
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
			case (Some(v), None) => v match {
				case s@(_: Short | _: Int | _: Long | _: Double) => numericReconcile(columnName, Some(s
					.asInstanceOf[Number].doubleValue), None)
				case s@(_: BigInt | _: BigDecimal) => numericReconcile(columnName, Some(s.asInstanceOf[BigDecimal]), None)
				case s@(_: Date | _: Timestamp | _: String) => stringLikeReconcile(columnName, Some(s), None)
			}
			case (None, Some(v)) => v match {
				case t@(_: Short | _: Int | _: Long | _: Double) => numericReconcile(columnName, None, Some(t
					.asInstanceOf[Number].doubleValue))
				case t@(_: BigInt | _: BigDecimal) => numericReconcile(columnName, None, Some(t.asInstanceOf[BigDecimal]))
				case t@(_: Date | _: Timestamp | _: String) =>
					stringLikeReconcile(columnName, None, Some(t))
			}
		}
	}

	private def numericReconcile[N](columnName: String, source: Option[N], target: Option[N])(implicit n: Fractional[N]): NumberColumn[N] = {
		import n._
		val valueToN = (value: Option[N]) => value match {
			case Some(v) => if(toDouble(v) == 0) (v, true) else (v, false)
			case None => (fromInt(0), false)
		}
		val (sourceN, isSZero) = valueToN(source)
		val (targetN, isTZero) = valueToN(target)
		val different: N = sourceN - targetN
		val deviation: Double = if(isSZero && isTZero) 0
			else if(equiv(abs(different), targetN)) - 100
			else setScale(toDouble(different / sourceN) * 100)
		val isMatched: Boolean = equiv(sourceN, targetN)

		val actualV = (v: N, isZero: Boolean) => if(toDouble(v) == 0 && !isZero) None else Some(v)
		NumberColumn(columnName, actualV(sourceN, isSZero), actualV(targetN, isTZero), different, deviation, isMatched)
	}

	private def stringLikeReconcile[S](columnName: String, source: Option[S], target: Option[S]): StringLikeColumn[S] = {
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
