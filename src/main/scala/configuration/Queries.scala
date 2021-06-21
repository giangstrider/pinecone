package configuration

import pureconfig._
import pureconfig.generic.auto._
import java.io.File


sealed trait SQLMethod
case class FileBase(sqlFile: File) extends SQLMethod
case class TextBase(sqlQuery: String) extends SQLMethod

case class QueryConf(
    queryKey: String,
    sourceName: String,
    sourceQuery: SQLMethod,
    targetName: String,
    targetQuery: SQLMethod,
    acceptedDeviation: Double = 1,
    reconcileKey: List[String]
)

case class QueriesConf(
    queries: List[QueryConf]
)
