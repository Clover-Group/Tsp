package ru.itclover.tsp.streaming.mappers

import cats.Id
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.{Time, _}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

case class PatternProcessor[E: TimeExtractor, State, Out](
  pattern: Pattern[E, State, Out],
  patternIdAndSubunit: (Int, Int),
  eventsMaxGapMs: Long,
  initialState: () => State,
  writeTimeLogs: Boolean
) {

  private val log = Logger("PatternLogger")
  private var lastState: State = _
  private var lastTime: Time = Time(0)
  private val timeExtractor = implicitly[TimeExtractor[E]]
  log.debug(s"pattern: $pattern")

  def map(
    elements: Iterable[E]
  ): Iterable[Out] = {
    if (elements.isEmpty) {
      log.info("No elements to proccess")
      return List.empty
    }

    val firstElement = elements.head
    // if the last event occurred so long ago, clear the state
    if (lastState == null || timeExtractor(firstElement).toMillis - lastTime.toMillis > eventsMaxGapMs) {
      lastState = initialState()
    }

    // Split the different time sequences if they occurred in the same time window
    val sequences = PatternProcessor.splitByCondition(elements.toSeq)((next, prev) =>
      timeExtractor(next).toMillis - timeExtractor(prev).toMillis > eventsMaxGapMs
    )

    val machine = StateMachine[Id]

    val data = mutable.ListBuffer.empty[Out]

    val consume: IdxValue[Out] => Unit = x => x.value.foreach(v => data.append(v))

    val seedStates = lastState +: Stream.continually(initialState())

    // this step has side-effect = it calls `consume` for each output event. We need to process
    // events sequentually, that's why I use foldLeft here
    lastState = sequences.zip(seedStates).foldLeft(initialState()) { case (_, (events, seedState)) =>
      machine.run(pattern, patternIdAndSubunit, events, seedState, consume, writeTimeLogs = writeTimeLogs)
    }

    lastTime = elements.lastOption.map(timeExtractor(_)).getOrElse(Time(0))

    data.toList
  }

  def getState: State = lastState
}

object PatternProcessor {

  val currentEventTsMetric = "currentEventTs"

  /** Splits a list into a list of fragments, the boundaries are determined by the given predicate E.g.
    * `splitByCondition(List(1,2,3,5,8,9,12), (x, y) => (x - y) > 2) == List(List(1,2,3,5),List(8,9),List(12)`
    *
    * @param elements
    *   initial sequence
    * @param pred
    *   condition between the next and previous elements (in this order)
    * @tparam T
    *   Element type
    * @return
    *   List of chunks
    */
  def splitByCondition[T](elements: Seq[T])(pred: (T, T) => Boolean): List[Seq[T]] =
    if (elements.length < 2) {
      List(elements)
    } else {
      val results = ListBuffer(ListBuffer(elements.head))
      elements.sliding(2).foreach { e =>
        val prev = e.head
        val cur = e(1)
        if (pred(cur, prev)) {
          results += ListBuffer(cur)
        } else {
          results.lastOption.getOrElse(sys.error("Empty result sequence - something went wrong")) += cur
        }
      }
      results.map(_.toSeq).toList
    }

}
