package parser

import org.scalatest.flatspec.AnyFlatSpec
import PineconeSQLUtil._


class PineconeSQLParserTest extends AnyFlatSpec {
	val expectedResultOfDateFunc = s"= ${getDateObject(1)}"

	"A SQL Query" should "able to parse function inside $WHERE{} indicator" in {
		val result = PineconeSQLProcessor("$where{=get_date(1)}")
		assert(expectedResultOfDateFunc === result)
	}

	it should "also able to parse function inside ${} indicator" in {
		val result = PineconeSQLProcessor("${=get_date(1)}")
		assert(expectedResultOfDateFunc === result)
	}

	it should "also able to parse function with NEGATIVE offset inside ${} indicator" in {
		val expectedResult = s"= ${getDateObject(-1)}"
		val result = PineconeSQLProcessor("${=get_date(-1)}")
		assert(expectedResult === result)
	}

	it should "able to parse BETWEEN function with $WHERE{} for past 7 days" in {
		val resultStartDate = getDateObject(7)
		val resultEndDate = getDateObject()
		val expectedResult = s"BETWEEN $resultStartDate AND $resultEndDate"
		val result = PineconeSQLProcessor("$where{BETWEEN(GET_DATE(7), GET_DATE())}")
		assert(expectedResult === result)
	}

	it should "failure to parse BETWEEN function with ${}" in {
		assertThrows[Exception] {
			PineconeSQLProcessor("${BETWEEN(GET_DATE(7), GET_DATE())}")
		}
	}
}
