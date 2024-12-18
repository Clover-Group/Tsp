package ru.itclover.tsp.benchmarking

import cats._
import org.scalatest.flatspec._

import org.scalatest.matchers.should._
import ru.itclover.tsp.core._
import ru.itclover.tsp.dsl.{ASTPatternGenerator, TestEvents}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

// This test uses Any values.
@SuppressWarnings(Array("org.wartremover.warts.Any"))
class FootprintBench extends AnyFlatSpec with Matchers:

  import TestEvents._

  val fieldsClasses = Map(
    "intSensor"     -> ClassTag.Int,
    "longSensor"    -> ClassTag.Long,
    "boolSensor"    -> ClassTag.Boolean,
    "doubleSensor1" -> ClassTag.Double,
    "doubleSensor2" -> ClassTag.Double
  )

  def process[T, S](pattern: Pattern[TestEvent, S, T], events: Seq[TestEvent]): Long =
    val start = System.nanoTime()
    val sm = StateMachine[Id]
    val initialState = pattern.initialState()
    val collect = new ArrayBuffer[(Long, Long)](events.size)
    val _ =
      sm.run(pattern, (1, 2), events, initialState, (x: IdxValue[T]) => { val _ = collect += (x.start -> x.end) }, 1000)
    val time = (System.nanoTime() - start) / 1000000
    println(time)
    time

  def repeat[T, S](times: Int, amount: Int, pattern: Pattern[TestEvent, S, T]): Long =
    val events = (1 to amount).map(l => TestEvent(l.toLong * 1000, 1, 1, boolSensor = true, 1.0, 2.0))
    val ts = (1 to times).map(_ => { val t = process(pattern, events); t }).sum
    ts / times

  it should "benchmark" in:
    given Conversion[String, String] = _.toString

    val gen = new ASTPatternGenerator[TestEvent, String, Any]

    val patternString = gen
      .build(
        "intSensor > 0 for 720 sec",
        0.0,
        1000L,
        fieldsClasses
      )
      .map(_._1)
      .getOrElse(???)

//    val optimizedPattern = new Optimizer[TestEvent].optimize(patternString)
    val actualTime = repeat(5, 1000, patternString)
    println(actualTime)
