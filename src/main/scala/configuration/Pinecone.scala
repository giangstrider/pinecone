package configuration

import pureconfig.ConfigSource
import pureconfig.error.ConfigReaderFailure
import pureconfig.generic.auto._


case class PineconeConf(
    database: PineconeDatabaseConf,
    proxy: PineconeProxyConf,
    databaseSupportedDriver: Map[String, String]
)

case class PineconeDatabaseConf(
    connectionName: String,
    schemaName: String,
    databaseName: String
)

case class PineconeProxyConf(
    useProxy: Boolean,
    httpProxy: Option[String],
	httpPort: Option[String],
	httpsProxy: Option[String],
	httpsPort: Option[String]
)

case class ConnectionConf(
	jdbc: Map[String, Map[String, String]]
)


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
}
