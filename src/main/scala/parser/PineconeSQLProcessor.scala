package parser


object PineconeSQLProcessor extends PineconeSQLParser {
	def apply(statementQuery: String): String = {
		parse(finalStatement, statementQuery.toUpperCase) match {
			case Success(matched, _) => matched
			case Failure(message, _) => throw new Exception(s"Parsing FAILURE: ${message}")
			case Error(message, _) => throw new Exception(s"Parsing ERROR: ${message}")
		}
	}
}
