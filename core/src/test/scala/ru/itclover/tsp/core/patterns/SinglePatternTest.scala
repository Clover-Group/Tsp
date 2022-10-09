// This runs FSM for all patterns using a one-entry queues

package ru.itclover.tsp.core.patterns

import cats.Id
import org.scalatest.flatspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.core._
import ru.itclover.tsp.core.fixtures.Common._
import ru.itclover.tsp.core.fixtures.Event
import ru.itclover.tsp.core.io.{Decoder, Extractor}

class SinglePatternTest extends AnyFlatSpec with Matchers {
//todo write tests!
  def processEvent[A](e: Event[A]): Result[A] = Result.succ(e.row)

  it should "process SimplePattern correctly" in {

    val pat = new SimplePattern[EInt, Int](_ => processEvent(event))(Event.extractor)

    val _ = StateMachine[Id].run(pat, Seq(event), pat.initialState())
  }

  it should "process ExtractingPattern correctly" in {

    // Decoder Instance
    implicit val dec: Decoder[Int, Int] = ((v: Int) => 2 * v)

    // Pattern Extractor
    implicit val MyExtractor: Extractor[EInt, Symbol, Int] = new Extractor[EInt, Symbol, Int] {
      def apply[T](a: EInt, sym: Symbol)(implicit d: Decoder[Int, T]): T = a.row
    }

    //val pat = new ExtractingPattern[EInt, Symbol, Int, Int, Int] ('and, 'or)(extractor, MyExtractor, dec)
    val pat = new ExtractingPattern('and)(Event.extractor, MyExtractor, dec)

    val _ = StateMachine[Id].run(pat, Seq(event), pat.initialState())
  }

}
