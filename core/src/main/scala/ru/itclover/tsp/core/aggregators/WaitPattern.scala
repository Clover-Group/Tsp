package ru.itclover.tsp.core.aggregators

import cats.instances.option.*
import com.typesafe.scalalogging.Logger
import ru.itclover.tsp.core.IdxValue
import ru.itclover.tsp.core.PQueue
import ru.itclover.tsp.core.Pattern
import ru.itclover.tsp.core.Pattern.Idx
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.Pattern.QI
import ru.itclover.tsp.core.Time
import ru.itclover.tsp.core.Window
import ru.itclover.tsp.core.io.TimeExtractor

import scala.collection.mutable as m

/* Wait pattern */
case class WaitPattern[Event: IdxExtractor: TimeExtractor, S, T](
  override val inner: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, T, WaitAccumState[T]]:

  override def initialState(): AggregatorPState[S, T, WaitAccumState[T]] = AggregatorPState(
    inner.initialState(),
    innerQueue = PQueue.empty,
    astate = WaitAccumState(),
    indexTimeMap = m.Queue.empty
  )

case class WaitAccumState[T]() extends AccumState[T, T, WaitAccumState[T]]:

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
  ): (WaitAccumState[T], QI[T]) =
    log.debug("Wait operator is now redundant and deprecated, returning the inner results as-is");
    (WaitAccumState(), PQueue(idxValue))
