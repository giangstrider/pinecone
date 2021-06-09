package configuration

case class ConnectionConf(
	jdbc: Map[String, Map[String, String]]
)