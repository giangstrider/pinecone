package reconciliation

import configuration.Pinecone.pineconeConf
import reconciliation.QueryStage.{PrepareQuery, ReconciliationRecord}
import reconciliation.ReconcileTypedColumn.{NumberColumn, StringLikeColumn}

import java.util.Date
import java.sql.Timestamp


object RecordReconciliation {
	def reconcile(source: Option[List[QueryRecord]], target: Option[List[QueryRecord]], query: PrepareQuery): List[ReconciliationRecord] = {
		implicit val acceptedDeviation: Double = query.acceptedDeviation
		(source, target) match {
			case (Some(s), Some(t)) =>
				val groupRecords = (s ++ t).groupBy(_.reconcileKeyValue)
				val matchedPair = groupRecords.filter(_._2.size == 2).values.map(pair => {
					val columnsS = pair.head.columns
					val columnsT = pair.last.columns

					val reconcileTypedColumns = columnsS.zipWithIndex.map{case (sc, index) =>
						val tc = columnsT(index)
						val value = convertToReconcileColumn(sc.columnValue, tc.columnValue)
						val clazz = classReconcile(sc.columnValue.get, tc.columnValue.get)
						val meta = Some(metadataReconcile(sc.metadata, tc.metadata, clazz))
						val isReconcileKey = checkReconcileKey(query, Seq(sc.columnName, tc.columnName))
						ReconciledColumn(sc.columnName, tc.columnName, isReconcileKey, value, meta)
					}

					ReconciliationRecord(query.queryKey, reconcileTypedColumns)
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
					val pairAttributes = qc.columns.map(c => {
						val value = convertToReconcileColumn(c.columnValue, None)
						val isReconcileKey = checkReconcileKey(query, Seq(c.columnName))
						ReconciledColumn(c.columnName, "", isReconcileKey, value, None)
					})
					ReconciliationRecord(query.queryKey, pairAttributes)
				})

			case (None, Some(v)) =>	v.map(qc => {
					val pairAttributes = qc.columns.map(c => {
						val value = convertToReconcileColumn(None, c.columnValue)
						val isReconcileKey = checkReconcileKey(query, Seq(c.columnName))
						ReconciledColumn("", c.columnName, isReconcileKey, value, None)
					})
					ReconciliationRecord(query.queryKey, pairAttributes)
				})
		}
	}

	private def processMissingPair(records: List[QueryRecord], isTarget: Boolean, query: PrepareQuery)(implicit acceptedDeviation: Double): List[ReconciliationRecord] = {
		records.map(qc => {
			val pairAttributes = if (isTarget) qc.columns.map(c => {
				val value = convertToReconcileColumn(None, c.columnValue)
				val isReconcileKey = query.reconcileKey.contains(c.columnName)
				ReconciledColumn("", c.columnName, isReconcileKey, value, None)
			})
			else qc.columns.map(c => {
				val value = convertToReconcileColumn(c.columnValue, None)
				val isReconcileKey = query.reconcileKey.contains(c.columnName)
				ReconciledColumn(c.columnName, "", isReconcileKey, value, None)
			})
			ReconciliationRecord(query.queryKey, pairAttributes)
		})
	}

	private def convertToReconcileColumn(source: Option[Any], target: Option[Any])(implicit acceptedDeviation: Double): ReconcileTypedColumn = {
		val javaToScalaConversion = (vb: Any) => BigDecimal(vb.asInstanceOf[java.math.BigDecimal])

		(source, target) match {
			case (Some(sv), Some(tv)) =>
				(sv, tv) match {
				case v@(_:Short | _:Int | _:Long | _:Float | _:Double, _:Short | _:Int | _:Long | _:Float | _:Double) =>
					numericReconcile(Some(v._1.asInstanceOf[Number].doubleValue), Some(v._2.asInstanceOf[Number].doubleValue))
				case v@(_:java.math.BigInteger | _:java.math.BigDecimal, _:java.math.BigInteger | _:java.math.BigDecimal) =>

					numericReconcile(Some(javaToScalaConversion(v._1)), Some(javaToScalaConversion(v._2)))
				case _ => stringLikeReconcile(Some(sv), Some(tv))
			}
			case (Some(v), None) => v match {
				case s@(_: Short | _: Int | _: Long | _: Double) => numericReconcile(Some(s
					.asInstanceOf[Number].doubleValue), None)
				case s@(_: java.math.BigInteger | _: java.math.BigDecimal) => numericReconcile(Some(javaToScalaConversion(s)), None)
				case s@(_: Date | _: Timestamp | _: String) => stringLikeReconcile(Some(s), None)
			}
			case (None, Some(v)) => v match {
				case t@(_: Short | _: Int | _: Long | _: Double) => numericReconcile(None, Some(t
					.asInstanceOf[Number].doubleValue))
				case t@(_: java.math.BigInteger | _: java.math.BigDecimal) => numericReconcile(None, Some(javaToScalaConversion(t)))
				case t@(_: Date | _: Timestamp | _: String) =>
					stringLikeReconcile(None, Some(t))
			}
		}
	}

	private def numericReconcile[N](source: Option[N], target: Option[N])(implicit n: Fractional[N], acceptedDeviation: Double): NumberColumn[N] = {
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
		val isMetAcceptedDeviation: Boolean = deviation.abs <= acceptedDeviation
		NumberColumn(actualV(sourceN, isSZero), actualV(targetN, isTZero), different, deviation, isMetAcceptedDeviation, isMatched)
	}

	private def stringLikeReconcile[S](source: Option[S], target: Option[S]): StringLikeColumn[S] = {
		val isMatched: Boolean = if(source == target) true else false
		StringLikeColumn(source, target, isMatched)
	}

	def metadataReconcile(source: Option[QueryMetadataColumn], target: Option[QueryMetadataColumn], clazzCheck: ReconciledMetadataRecord): ReconcileMetadataColumn = {
		(source, target) match {
			case (Some(sv), Some(tv)) =>
				val checkDisplaySize = if(sv.displaySize == tv.displaySize) Convertible.Good else Convertible.Warning
				val checkPrecision = if(sv.precision == tv.precision) Convertible.Good else Convertible.Alert
				val checkScale = if(sv.scale == tv.scale) Convertible.Good else Convertible.Alert
				val checkCurrency = if(sv.isCurrency == tv.isCurrency) Convertible.Good else Convertible.MinorWarning
				val checkNullable = if(sv.isNullable == tv.isNullable) Convertible.Good else Convertible.Warning
				ReconcileMetadataColumn(
					clazzCheck,
					ReconciledMetadataRecord(sv.displaySize, tv.displaySize, checkDisplaySize),
					ReconciledMetadataRecord(sv.precision, tv.precision, checkPrecision),
					ReconciledMetadataRecord(sv.scale, tv.scale, checkScale),
					ReconciledMetadataRecord(sv.isCurrency, tv.isCurrency, checkCurrency),
					ReconciledMetadataRecord(sv.isNullable, tv.isNullable, checkNullable)
				)
		}
	}

	def classReconcile(source: Any, target: Any): ReconciledMetadataRecord = {
		if(source.getClass == target.getClass) return ReconciledMetadataRecord(source.getClass, target.getClass, Convertible.Good)

		val checkNumeric = (v: Any) => v.isInstanceOf[Int] || v.isInstanceOf[Long] || v.isInstanceOf[Short]
		val checkBigNumeric = (v: Any) => v.isInstanceOf[BigInt]
		val checkDecimal = (v: Any) => v.isInstanceOf[Float] || v.isInstanceOf[Double]
		val checkBigDecimal = (v: Any) => v.isInstanceOf[BigDecimal]

		val reconcileConvertible = (s: Any, t: Any) => (s, t) match {
			case  v@(_, _) if v._1.isInstanceOf[Number] && v._2.isInstanceOf[Number] =>
				if(checkNumeric(v._1) == checkNumeric(v._2) || checkDecimal(v._1) == checkDecimal(v._2)) Convertible.MinorWarning
				else if(checkNumeric(v._1) == checkBigNumeric(v._2) || checkDecimal(v._1) == checkBigDecimal(v._2)) Convertible.Warning
				else Convertible.Alert
			case _ => Convertible.Alert
		}

		val sourceCompareTarget = reconcileConvertible(source, target)
		val targetCompareSource = reconcileConvertible(target, source)
		val convertible = if (sourceCompareTarget == targetCompareSource || sourceCompareTarget > targetCompareSource)
			sourceCompareTarget
		else targetCompareSource
		ReconciledMetadataRecord(source.getClass, target.getClass, convertible)
	}

	private def checkReconcileKey(query: PrepareQuery, columnNames: Seq[String]): Boolean = {
		val listKeys = query.reconcileKey.map(_.toUpperCase)
		columnNames.exists(listKeys.contains)
	}

	private def setScale(number: Double): Double = {
		val decimalScale = pineconeConf.reconciliation.decimalScale
		BigDecimal(number).setScale(decimalScale, BigDecimal.RoundingMode.HALF_EVEN).doubleValue
	}



	def validateQueryColumn = ???
	//Reject column name appear more than one
}