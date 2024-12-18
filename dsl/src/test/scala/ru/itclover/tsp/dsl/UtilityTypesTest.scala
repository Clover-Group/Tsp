package ru.itclover.tsp.dsl

import org.scalatest.wordspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.dsl.UtilityTypes.ParseException

/** Class for testing utility types
  */
class UtilityTypesTest extends AnyWordSpec with Matchers:

  "retrieve errors" should:

    "from string" in:

      val testErrorString = "test exception message"
      val exception = ParseException.apply(testErrorString)

      exception.getMessage shouldBe testErrorString

    "from sequence" in:

      val testErrorSequence = Seq("Test #1", "Test #2", "Test #3")
      val exception = ParseException.apply(testErrorSequence)

      val expectedMessage = "Test #1\nTest #2\nTest #3"

      exception.getMessage shouldBe expectedMessage
