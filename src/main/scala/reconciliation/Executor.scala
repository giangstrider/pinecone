package reconciliation

import anorm.SqlParser

object Executor {
	private val reconQueriesParser = SqlParser.folder(Map.empty[String, Any]) { (map, value, meta) =>
		Right(map + (meta.column.alias.get -> value))
	}
}
