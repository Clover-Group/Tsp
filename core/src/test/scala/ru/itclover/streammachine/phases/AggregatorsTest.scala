package ru.itclover.streammachine.phases

import org.joda.time.{DateTime, Instant}
import org.joda.time.format.DateTimeFormatter
import org.scalatest.{FunSuite, Matchers, WordSpec}
import ru.itclover.streammachine.{Event, core}
import ru.itclover.streammachine.core.PhaseParser.Functions._
import ru.itclover.streammachine.core.{PhaseResult, TestPhase, Time, _}
import ru.itclover.streammachine.core.PhaseResult.{Failure, Stay, Success}
import ru.itclover.streammachine.core.Time.TimeExtractor
import java.time.Instant

import ru.itclover.streammachine.aggregators.AggregatorPhases.{Derivation, Segment, ToSegments}
import ru.itclover.streammachine.core.Time._
import ru.itclover.streammachine.phases.CombiningPhases.{And, TogetherParser}
import ru.itclover.streammachine.phases.NumericPhases._
import ru.itclover.streammachine.phases.TimePhases.Timer
import ru.itclover.streammachine.utils.ParserMatchers

import scala.concurrent.duration._


class AggregatorsTest extends WordSpec with ParserMatchers {


  "Timer phase" should {
    "work" in {
      checkOnTestEvents(
        (p: TestPhase[Double]) => p.timed(2.seconds, 2.seconds).map(_._1),
        staySuccesses,
        Seq(Failure(""), Success(2.0), Success(1.0), Success(3.0), Failure("Test"), Failure("Test"), Failure("Test"))
      )
    }
  }


  /*"Derivation phase" should {
    "Work!" in {
      val results = Stay #:: Success(1.0) #:: Success(2.0) #:: Success(1.0) #:: Success(3.0) #:: Stream.empty[PhaseResult[Double]]
      testOnFixedResults(
        p => Derivation(p),
        times.take(results.length).map(TestingEvent),
        results,
        Seq(Success(0.001), Success(-0.001), Success(0.002))
      )
    }
  }*/


  /*"IncludeStays phase" should {
    "work on stay-success" in {
      val wwsStream = Stay #:: Stay #:: Success(()) #:: Stream.empty[PhaseResult[Unit]]
      val stay_success = ToSegments(StreamResult[TimedEvent, Unit](wwsStream))

      val events = times.take(3).map(t => TimedEvent(0, t))
      val results = runRule(stay_success, events)

      results.length should equal(3)
      results(0) should not be an [Success[_]]
      results(1) should not be an [Success[_]]
      results(2) shouldBe a [Success[_]]

      val segmentLengthOpt = results(2) match {
        case Success(Segment(from, to)) => Some(to.toMillis - from.toMillis)
        case _ => None
      }

      segmentLengthOpt should not be empty
      segmentLengthOpt.get should equal(2000L)
    }

    "not work on stay-failure" in {
      val wwfStream = Stay #:: Stay #:: Failure("Test failure") #:: Stream.empty[PhaseResult[Unit]]
      val stay_failure = ToSegments(StreamResult[TimedEvent, Unit](wwfStream))

      val (result1, state1) = stay_failure.apply(TimedEvent(100, times.head), stay_failure.initialState)
      val (result2, state2) = stay_failure.apply(TimedEvent(200, times(1)), state1)
      val (result3, state3) = stay_failure.apply(TimedEvent(300, times(2)), state2)

      result1 should not be an [Failure]
      result2 should not be an [Failure]
      result3 shouldBe a [Failure]
    }
  }*/
}
