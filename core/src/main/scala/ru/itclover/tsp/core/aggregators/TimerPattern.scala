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
  override val window: Window,
  val eventsMaxGapMs: Long
) extends AccumPattern[Event, S, T, Boolean, TimerAccumState[T]]:

  override def initialState(): AggregatorPState[S, T, TimerAccumState[T]] = AggregatorPState(
    inner.initialState(),
    innerQueue = PQueue.empty,
    astate = TimerAccumState(None),
    indexTimeMap = m.ArrayDeque.empty
  )

case class TimerAccumState[T](
  successStarted: Option[(Idx, Time)]
) extends AccumState[T, Boolean, TimerAccumState[T]]:

  // val log = Logger(classOf[TimerAccumState[T]])

  @inline
  override def updated(
    window: Window,
    times: m.ArrayDeque[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (TimerAccumState[T], QI[Boolean]) =
    // log.debug(s"Current state: $this")
    // log.debug(s"Received event: $idxValue with times: $times")
    idxValue.value match

      case Fail =>
        // return fail for the whole event
        (TimerAccumState(None), PQueue(IdxValue(idxValue.start, idxValue.end, Result.fail)))
      case Wait =>
        // this should not happen normally, but we return wait for this thing too
        (TimerAccumState(None), PQueue(IdxValue(idxValue.start, idxValue.end, Result.wait)))
      case Succ(t) =>
        val successStart = successStarted.getOrElse(times.head)
        val waitEndTime = successStart._2.plus(window)
        val (waitOutputs, successOutputs) = takeWhileFromQueue(times)(_._2 < waitEndTime)
        val waitResult = createIdxValue(waitOutputs.headOption, waitOutputs.lastOption, Result.wait)
        val successResult = createIdxValue(successOutputs.headOption, successOutputs.lastOption, Result.succ(true))
        val newQueue =
          List(waitResult, successResult).filter(_.isDefined).map(_.get).foldLeft(PQueue.empty[Boolean])(_.enqueue(_))
        (TimerAccumState(Some(successStart)), newQueue)

  private def createIdxValue(
    optStart: Option[(Idx, Time)],
    optEnd: Option[(Idx, Time)],
    result: Result[Boolean]
  ): Option[IdxValue[Boolean]] =
    Apply[Option].map2(optStart, optEnd)((start, end) => IdxValue(start._1, end._1, result))
