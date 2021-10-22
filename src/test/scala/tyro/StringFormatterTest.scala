package tyro

import org.scalatest.flatspec.AnyFlatSpec

class StringFormatterTest extends AnyFlatSpec{
	"The String Formatter - given the sentence and the width" should "formats a paragraph into a fixed width without breaking word" in {
		val sentence = "The quick brown fox jumps over the lazy dog."
		val width = 14

		val output =
			"""
			  |The quick
			  |brown fox
			  |jumps over the
			  |lazy dog.""".stripMargin

		assert(output === StringFormatter.format(sentence, width))
	}

//	it should "formats a paragraph into a fixed width without breaking word" in {
//		val sentence = "The quick brown fox jumps over the lazy dog."
//		val width = 14
//
//		val output =
//			"""
//			  |The quick
//			  |brown fox
//			  |jumps over the
//			  |lazy dog.""".stripMargin
//
//		assert(output === StringFormatter.format(sentence, width))
//	}
}
