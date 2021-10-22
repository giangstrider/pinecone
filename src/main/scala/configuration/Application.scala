package configuration


case class PineconeConf(
    database: PineconeDatabaseConf,
    proxy: PineconeProxyConf,
    databaseSupportedDriver: Map[String, String],
    sqlTemplate: PineconeSQLTemplateConf,
    reconciliation: PineconeReconciliationConf,
    concurrency: PineconeConcurrencyConf,
    notification: PineconeNotificationConf
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
    pickStrategy: String,
    decimalScale: Int
)

case class PineconeConcurrencyConf(
	fixedPoolSize: Int
)

case class PineconeNotificationConf (
	alertSns: Boolean,
	alertEmail: Boolean,
	reportEmail: Boolean,
	subcriberGroups: Option[Map[String, Map[String, String]]],
	emailConf: Option[PineconeEmailConf]
)

case class PineconeEmailConf (
    smtpServer: String
)