package parser

import scala.util.parsing.combinator._
import PineconeSQLUtil._

class PineconeSQLParser extends RegexParsers {
	def number: Parser[Long] = """-?(0|[1-9]\d*)""".r ^^ { _.toLong }
	def fixedDateParameter: Parser[String] = "([12]\\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\\d|3[01]))".r

	def conditionOpen: Parser[String] = "${"
	def conditionGroupOpen: Parser[String] = "$WHERE{"
	def generalOpen: Parser[String] = conditionOpen | conditionGroupOpen
	def conditionClose: Parser[String] = "}"

	def singleOperator: Parser[String] = "=" | "!=" | ">" | "<" | ">=" | "<="
	def betweenOperator: Parser[String] = "BETWEEN"
	def operatorOpen: Parser[String] = singleOperator | betweenOperator

	def dateFunction: Parser[String] = "GET_DATE" | "GET_TIMESTAMP"
	def dateOffsetParameter = dateFunction ~ "(" ~ number ~ ")" ^^ {
		case dateFunc ~ openBracket ~ offsetParam ~ closeBracket =>
			if(dateFunc.contains("DATE")) getDateObject(offsetParam) else getTimestampObject(offsetParam)
	}
	def dateDefaultOffsetParameter = dateFunction ~ "()" ^^ {
		case dateFunc ~ bracket => if(dateFunc.contains("DATE")) getDateObject() else getTimestampObject()
	}
	def dateFunctionWithParam = dateOffsetParameter | dateDefaultOffsetParameter
	def dateParam = dateFunctionWithParam | fixedDateParameter


	def singleStatement = singleOperator ~ dateParam ^^ {
		case operator ~ param => s"${operator} ${param}"
	}
	def betweenStatement = betweenOperator ~ "(" ~ dateParam ~ "," ~ dateParam ~ ")" ^^ {
		case operator ~ openBracket ~ startParam ~ comma ~ endParam ~ closeBracket =>
			s"BETWEEN $startParam AND $endParam"
	}

	def finalStatement = (
			(generalOpen ~ singleStatement) | (conditionGroupOpen ~ betweenStatement)
		) ~ conditionClose ^^ {
		case open ~ statement ~ close => statement
	}
}