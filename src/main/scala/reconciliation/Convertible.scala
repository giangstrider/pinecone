package reconciliation


object Convertible extends Enumeration {
	type LEVEL = Value

	val Alert = Value(1, "Data type between Source and Target not convertible")
	val Warning = Value(2, "Data type between Source and Target may not fully represent the same")
	val MinorWarning = Value(3, "Data type between Source and Target may different but almost same for most of case")
	val Good = Value(4, "Data type between Source and Target exactly match")
}
