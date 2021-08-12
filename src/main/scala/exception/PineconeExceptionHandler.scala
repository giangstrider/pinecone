package exception

import com.typesafe.scalalogging.Logger

import java.sql.SQLException
import scala.util.{Failure, Success, Try}


object PineconeExceptionHandler {
	def sqlException[T](tryHandling: Try[T], successMessage: String, logger: Logger): T = {
		tryHandling match {
			case Success(s) => logger.info(successMessage); s
			case Failure(sqlEx: SQLException) => exceptionStop(sqlEx)
		}
	}

	def exceptionStop(exception: Throwable): Nothing = {
		throw new Exception(exception.getMessage)
	}

	def exceptionStop(exceptionMessage: String): Nothing = {
		throw new Exception(exceptionMessage)
	}
}
