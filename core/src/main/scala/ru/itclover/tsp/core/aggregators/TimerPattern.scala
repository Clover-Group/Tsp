package ru.itclover.tsp.core.aggregators

import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.{Time, Window, _}
import cats.instances.option._
import cats.Apply

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}

/* Timer */
case class TimerPattern[Event: IdxExtractor: TimeExtractor, S, T](
  override val inner: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, Unit, TimerAccumState[T]] {
  override def initialState(): AggregatorPState[S, T, TimerAccumState[T]] = AggregatorPState(
    inner.initialState(),
    innerQueue = PQueue.empty,
    astate = TimerAccumState(m.Queue.empty),
    indexTimeMap = m.Queue.empty
  )
}

case class TimerAccumState[T](windowQueue: m.Queue[(Idx, Time)]) extends AccumState[T, Unit] {

  @inline
  override def updated(
    window: Window,
    times: m.Queue[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (TimerAccumState[T], QI[Unit]) = {

    val (updatedWindowQueue, newOptResult) = idxValue.value match {
      // clean queue in case of fail. Return fails for all events in queue
      case Fail =>
        val newOptResult = createIdxValue(windowQueue.headOption.orElse(times.headOption), times.lastOption, Fail)
        (m.Queue.empty[(Idx, Time)], newOptResult)
      // in case of Success we need to return Success for all events in window older than window size.
      case Succ(_) =>
        val end: Time = times.last._2 // time corresponding to the idxValue.end

        // don't use ++ here, slow!
        val windowQueueWithNewPoints = times.foldLeft(windowQueue) { case (a, b) => a.enqueue(b); a }

        val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueueWithNewPoints) {
          case (_, t) => t.plus(window) <= end
        }

        val newOptResult = createIdxValue(outputs.headOption, outputs.lastOption, Result.succUnit)
        (updatedWindowQueue, newOptResult)
    }
    (TimerAccumState(updatedWindowQueue), newOptResult.map(PQueue.apply).getOrElse(PQueue.empty))
  }

  private def createIdxValue(
    optStart: Option[(Idx, Time)],
    optEnd: Option[(Idx, Time)],
    result: Result[Unit]
  ): Option[IdxValue[Unit]] =
    Apply[Option].map2(optStart, optEnd)((start, end) => IdxValue(start._1, end._1, result))
}
