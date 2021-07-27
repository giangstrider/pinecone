package parser


object PineconeSQLProcessor extends PineconeSQLParser {
	def apply(statementQuery: String): String = {
		val regexPattern = "(\\$|\\$WHERE)\\{.*?\\}".r
		regexPattern.findAllIn(statementQuery).foldLeft(statementQuery) {
			(amendQuery, re) => amendQuery.replace(re, parseQuery(re))
		}
	}

	private def parseQuery(query: String): String = {
		parse(finalStatement, query.toUpperCase) match {
			case Success(matched, _) => matched
			case Failure(message, _) => throw new Exception(s"Parsing FAILURE: ${message} - Query: ${query.toUpperCase}")
			case Error(message, _) => throw new Exception(s"Parsing ERROR: ${message}")
		}
	}
}
