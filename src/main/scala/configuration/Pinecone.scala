package configuration

import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailure
import pureconfig.generic.auto._


object Pinecone {
	def pineconeConf(): PineconeConf = {
		ConfigSource.resources("pinecone.conf").load[PineconeConf] match {
			case Right(conf) => conf
		}
	}

	def connectionConf(): Map[String, Map[String, String]] = {
		ConfigSource.resources("connection.conf").load[ConnectionConf] match {
			case Right(conf) => conf.jdbc
		}
	}

	def queriesConf(): QueriesConf = {
		ConfigSource.resources("queries.conf").load[QueriesConf] match {
			case Right(conf) => conf
		}
	}
}
