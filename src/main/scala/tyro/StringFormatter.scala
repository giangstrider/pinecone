package tyro

object StringFormatter {
	def format(sentence: String, width: Int): String = {
		val splittedWords = sentence.split(" ")
		var currentLength: Int = 0
		val space = 1
		var output = ""

		splittedWords.map(e => {
			if(currentLength == 0) {
				currentLength = e.length + space
				val result = appendOutput(currentLength, width, output, e.length, e)
				output = result._1
				currentLength = result._2
			} else {
				val currentElementLength = e.length
				currentLength = currentLength + currentElementLength
				val result = appendOutput(currentLength, width, output, e.length, e)
				output = result._1
				currentLength = result._2
			}
		})

		output
	}

	def appendOutput(totalCurrentLength: Int, width: Int, currentOutput: String, currentLength: Int, currentElement: String): (String, Int) = {
		var output = currentOutput
		if (totalCurrentLength > width) {
			output += "---"
			(output, 0)
		} else {
			output += currentElement
			output += " "
			(output, currentLength)
		}
	}
}
