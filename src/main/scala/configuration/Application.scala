package configuration


case class PineconeConf(
    database: PineconeDatabaseConf,
    proxy: PineconeProxyConf,
    databaseSupportedDriver: Map[String, String],
    sqlTemplate: PineconeSQLTemplateConf,
    reconciliation: PineconeReconciliationConf
)

case class PineconeDatabaseConf(
    connectionName: String,
    schemaName: String,
    databaseName: String,
    queriesTableName: String
)

case class PineconeProxyConf(
    useProxy: Boolean,
    httpProxy: Option[String],
	httpPort: Option[String],
	httpsProxy: Option[String],
	httpsPort: Option[String]
)

case class PineconeSQLTemplateConf(
    timeZone: String,
    dateFormat: String,
    timestampFormat: String
)

case class PineconeReconciliationConf(
    strategy: String,
    decimalScale: Int
)
