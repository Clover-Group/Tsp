package ru.itclover.tsp.core.aggregators

import cats.instances.option._
import cats.Apply
import ru.itclover.tsp.core.Pattern.{Idx, IdxExtractor, QI}
import ru.itclover.tsp.core.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.core.{IdxValue, PQueue, Pattern, Time, Window}
import ru.itclover.tsp.core.io.TimeExtractor

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}
import scala.util.Try
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.core.Result
import ru.itclover.tsp.core.Fail

/* Wait pattern */
case class WaitPattern[Event: IdxExtractor: TimeExtractor, S, T](
  override val inner: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, T, WaitAccumState[T]] {

  override def initialState(): AggregatorPState[S, T, WaitAccumState[T]] = AggregatorPState(
    inner.initialState(),
    innerQueue = PQueue.empty,
    astate = WaitAccumState(m.Queue.empty, lastIndex = 0),
    indexTimeMap = m.Queue.empty
  )

}

// Here, head and last are guaranteed to work, so suppress warnings for them
@SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
case class WaitAccumState[T](windowQueue: m.ArrayDeque[(Idx, Time)], lastIndex: Idx)
    extends AccumState[T, T, WaitAccumState[T]] {

  val log = Logger[WaitAccumState[T]]

  /** This method is called for each IdxValue produced by inner patterns.
    *
    * @param window
    *   \- defines time window for accumulation.
    * @param times
    *   \- contains mapping Idx->Time for all events with Idx in [idxValue.start, idxValue.end]. Guaranteed to be
    *   non-empty.
    * @param idxValue
    *   \- result from inner pattern.
    * @param lastIndex
    *   \- last index for which the result has already returned
    * @return
    *   Tuple of updated state and queue of results to be emitted from this pattern.
    */
  @inline
  override def updated(
    window: Window,
    times: m.ArrayDeque[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (WaitAccumState[T], QI[T]) = {
    // println(s"WAIT: window = ${window.toMillis}, times = ${times.head} to ${times.last}, iv = $idxValue")
    val (newLastIndex, newWindowQueue, newOutputQueue) =
      times.foldLeft(Tuple3(lastIndex, windowQueue, PQueue.empty[T])) {
        case ((lastIndex, windowQueue, outputQueue), (idx, time)) =>
          addOnePoint(idx, time, idxValue.value, window, lastIndex, windowQueue, outputQueue)
      }
    // println(s"WAIT: wq = $newWindowQueue, oq = $newOutputQueue")
    WaitAccumState(newWindowQueue, newLastIndex) -> newOutputQueue
  }

  @inline
  def addOnePoint(
    idx: Idx,
    time: Time,
    value: Result[T],
    window: Window,
    lastIndex: Idx,
    windowQueue: m.ArrayDeque[(Idx, Time)],
    outputQueue: QI[T]
  ): (Idx, m.ArrayDeque[(Idx, Time)], QI[T]) = {
    if (value.isSuccess) {
      // if success arrived, clear the queue and return success for everything after lastIndex except for outdated values
      val (outdated, newQueue) = takeWhileFromQueue(windowQueue)(_._2.plus(window) < time)
      windowQueue.clear()
      if (outdated.isEmpty) {
        (idx, windowQueue, outputQueue.enqueue(IdxValue(lastIndex + 1, idx, value)))
      } else {
        (
          idx,
          windowQueue,
          outputQueue.enqueue(
            IdxValue(lastIndex + 1, outdated.last._1, Fail),
            IdxValue(outdated.last._1 + 1, idx, value)
          )
        )
      }
    } else {
      // if fail arrived
      // first, add the new element to the queue
      windowQueue.addOne((idx, time))
      // take outdated elements from the window queue
      val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue)(_._2.plus(window) < time)
      if (outputs.nonEmpty) {
        // set the new last index if there are outputs (i.e. there is something to return)
        (
          outputs.last._1,
          updatedWindowQueue,
          outputQueue.enqueue(IdxValue(outputs.head._1, outputs.last._1, Result.fail))
        )
      } else {
        // keep the old last index
        (lastIndex, updatedWindowQueue, outputQueue)
      }
    }
  }

}
