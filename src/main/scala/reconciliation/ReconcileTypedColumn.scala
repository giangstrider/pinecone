package reconciliation

sealed trait ReconcileTypedColumn {
	def columnName: String
	def isMatched: Boolean
}

object ReconcileTypedColumn {
	case class NumberColumn[N](columnName: String, source: N, target: N, different: N, deviation: Double,
	                        isMatched: Boolean) extends ReconcileTypedColumn
	case class StringLikeColumn[S](columnName: String, source: Option[S], target: Option[S],
	                              isMatched: Boolean)
		extends
		ReconcileTypedColumn

	def apply[N](columnName: String, source: N, target: N, different: N, deviation: Double,
	          isMatched: Boolean) =
		NumberColumn(columnName, source, target, different, deviation, isMatched)

	def apply[S](columnName: String, source: Option[S], target: Option[S], isMatched: Boolean) =
		StringLikeColumn(columnName, source, target, isMatched)
}