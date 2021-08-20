package parser


object PineconeSQLProcessor extends PineconeSQLParser {
	def apply(statementQuery: String, timeRollingParam: Option[String]): String = {
		val regexPattern = "(\\$|\\$TIME)\\{.*?\\}".r
		regexPattern.findAllIn(statementQuery).foldLeft(statementQuery) {
			(amendQuery, re) =>
				timeRollingParam match {
					case Some(p) => if(re.contains("$TIME")) amendQuery.replace(re, parseQuery(p))
					else amendQuery.replace(re, parseQuery(re))
					case None => amendQuery.replace(re, parseQuery(re))
				}
		}
	}

	private def parseQuery(pattern: String): String = {
		parse(finalStatement, pattern.toUpperCase) match {
			case Success(matched, _) => matched
			case Failure(message, _) => throw new Exception(s"Parsing FAILURE: ${message} - Pattern: ${pattern.toUpperCase}")
			case Error(message, _) => throw new Exception(s"Parsing ERROR: ${message}")
		}
	}
}
