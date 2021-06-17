package exception


object PineconeExceptionHandler {
	def exceptionStop(exception: Throwable) = {
		throw new Exception(exception.getMessage)
	}

	def exceptionStop(exceptionMessage: String) = {
		throw new Exception(exceptionMessage)
	}
}
