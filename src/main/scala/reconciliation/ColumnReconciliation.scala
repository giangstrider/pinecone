package reconciliation


object ColumnReconciliation {
	def classReconcile(columnName: String, source: Option[Any], target: Option[Any]): ReconcileMetadataColumn = {
		(source, target) match {
			case (Some(sv), Some(tv)) =>
				if(sv.getClass == tv.getClass)
					return ReconcileClassColumn(columnName, Some(sv.getClass), Some(tv.getClass), Convertible.Good, true)

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

				val sourceCompareTarget = reconcileConvertible(sv, tv)
				val targetCompareSource = reconcileConvertible(tv, sv)
				val convertibleLevel = if (sourceCompareTarget == targetCompareSource || sourceCompareTarget > targetCompareSource) sourceCompareTarget
					else targetCompareSource

				ReconcileClassColumn(columnName, Some(sv.getClass), Some(tv.getClass), convertibleLevel, false)
			case (Some(v), None) => ReconcileClassColumn(columnName, Some(v.getClass), None, Convertible.Alert, false)
			case (None, Some(v)) => ReconcileClassColumn(columnName, None, Some(v.getClass), Convertible.Alert, false)
			case (None, None) => ReconcileClassColumn(columnName, None, None, Convertible.Alert, false)
		}
	}

	def nullableReconcile(columnName: String, source: Boolean, target: Boolean) =
		ReconcileNullableColumn(columnName, source, target, source == target)
}
