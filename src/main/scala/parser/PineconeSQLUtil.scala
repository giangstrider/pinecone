package parser

import configuration.Pinecone.pineconeConf

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId}

object PineconeSQLUtil {
	private val timezone = ZoneId.of(pineconeConf.sqlTemplate.timeZone)
	private val dateFormat = pineconeConf.sqlTemplate.dateFormat
	private val dateTimeFormat = pineconeConf.sqlTemplate.timestampFormat

	def getDateObject(days: Long = 0): String = {
		val localDate = LocalDate.now(timezone).minusDays(days * -1)
		val formatter = DateTimeFormatter.ofPattern(dateFormat)
		s"'${localDate.format(formatter)}'"
	}

	def getTimestampObject(seconds: Long = 0): String = {
		val localDateTime = LocalDateTime.now(timezone).minusSeconds(seconds * -1)
		val formatter = DateTimeFormatter.ofPattern(dateTimeFormat)
		s"'${localDateTime.format(formatter)}'"
	}

	def getExtractedTimeFromDate(func: String, date: String): String = {
		val pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd")
		val localDate = LocalDate.parse(date.replace("'", ""), pattern)
		val result = func match {
			case "YEAR" => localDate.getYear
			case "MONTH" => localDate.getMonthValue
			case "DAY" => localDate.getDayOfYear
		}
		result.toString
	}
}
