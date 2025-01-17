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

/* Wait pattern */
case class WaitPattern[Event: IdxExtractor: TimeExtractor, S, T](
  override val inner: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, T, WaitAccumState[T]] {
  override def initialState(): AggregatorPState[S, T, WaitAccumState[T]] = AggregatorPState(
    inner.initialState(),
    innerQueue = PQueue.empty,
    astate = WaitAccumState(m.Queue.empty, lastFail = false, lastTime = (0, Time(0))),
    indexTimeMap = m.Queue.empty
  )
}

// Here, head and last are guaranteed to work, so suppress warnings for them
@SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
case class WaitAccumState[T](windowQueue: m.ArrayDeque[(Idx, Time)], lastFail: Boolean, lastTime: (Idx, Time))
    extends AccumState[T, T, WaitAccumState[T]] {

  /** This method is called for each IdxValue produced by inner patterns.
    *
    * @param window   - defines time window for accumulation.
    * @param times    - contains mapping Idx->Time for all events with Idx in [idxValue.start, idxValue.end].
    *                 Guaranteed to be non-empty.
    * @param idxValue - result from inner pattern.
    * @return Tuple of updated state and queue of results to be emitted from this pattern.
    */
  @inline
  override def updated(
    window: Window,
    times: m.ArrayDeque[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (WaitAccumState[T], QI[T]) = {

    // TODO: Temp, preventing failures if wrong idxValue arrived (investigate this better, why it happens)
    if (times.nonEmpty && idxValue.end >= idxValue.start) {
      val start = if (lastFail) times.head._2.minus(window) else times.head._2
      val end = if (idxValue.value.isFail) times.last._2.minus(window) else times.last._2

      // don't use ++ here, slow!
      val windowQueueWithNewPoints = times.foldLeft(windowQueue) { case (a, b) => a.append(b); a }

      val cleanedWindowQueue = {
        while (windowQueueWithNewPoints.length > 1 && windowQueueWithNewPoints.toArray.apply(1)._2 < start) {
          windowQueueWithNewPoints.removeHeadOption()
        }
        windowQueueWithNewPoints
      }

      val (outputs, updatedWindowQueue) = takeWhileFromQueue(cleanedWindowQueue) {
        case (_, t) => t <= end
      }

      val waitStart =
        if (lastTime._2.toMillis != 0 && Try(outputs.head._2.plus(window) <= outputs.last._2).getOrElse(false)) {
          outputs.headOption
        } else {
          Some(cleanedWindowQueue.lastOption.getOrElse(lastTime))
        }
      val waitEnd = outputs.lastOption

      val newOptResult = Apply[Option]
        .map2(waitStart, waitEnd) { (s, e) =>
          if (s._1 <= e._1) Some(IdxValue(s._1, e._1, idxValue.value)) else None
        }
        .flatten

      (
        WaitAccumState(updatedWindowQueue, idxValue.value.isFail, times.last),
        newOptResult.map(PQueue.apply).getOrElse(PQueue.empty)
      )
    } else {
      (this, PQueue.empty)
    }
  }

}
