package reconciliation

sealed trait ReconcileTypedColumn {
	def isMatched: Boolean
}

object ReconcileTypedColumn {
	case class NumberColumn[N](source: Option[N], target: Option[N], different: N,
	                           deviation: Double, isMetAcceptedDeviation: Boolean,
	                        isMatched: Boolean) extends ReconcileTypedColumn
	case class StringLikeColumn[S](source: Option[S], target: Option[S],
	                              isMatched: Boolean)
		extends
		ReconcileTypedColumn

	def apply[N](source: Option[N], target: Option[N], different: N, deviation: Double,
	          isMetAcceptedDeviation: Boolean, isMatched: Boolean) =
		NumberColumn(source, target, different, deviation, isMetAcceptedDeviation, isMatched)

	def apply[S](source: Option[S], target: Option[S], isMatched: Boolean) =
		StringLikeColumn(source, target, isMatched)
}