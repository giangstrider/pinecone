package parser

import configuration.Pinecone.pineconeConf

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId}

object PineconeSQLUtil {
	private val timezone = ZoneId.of(pineconeConf.sqlTemplate.timeZone)
	private val dateFormat = pineconeConf.sqlTemplate.dateFormat
	private val dateTimeFormat = pineconeConf.sqlTemplate.timestampFormat

	def getDateObject(days: Long = 0): String = {
		val localDate = LocalDate.now(timezone).minusDays(days)
		val formatter = DateTimeFormatter.ofPattern(dateFormat)
		localDate.format(formatter)
	}

	def getTimestampObject(seconds: Long = 0): String = {
		val localDateTime = LocalDateTime.now(timezone).minusSeconds(seconds)
		val formatter = DateTimeFormatter.ofPattern(dateTimeFormat)
		localDateTime.format(formatter)
	}
}
