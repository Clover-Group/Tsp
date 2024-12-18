package ru.itclover.tsp.core.aggregators

import ru.itclover.tsp.core.Pattern._
import ru.itclover.tsp.core.QueueUtils.takeWhileFromQueue
import ru.itclover.tsp.core.io.TimeExtractor
import ru.itclover.tsp.core.{Pattern, Time, Window, _}

import scala.Ordering.Implicits._
import scala.collection.{mutable => m}

//todo docs
//todo simplify
case class WindowStatistic[Event: IdxExtractor: TimeExtractor, S, T](
  override val inner: Pattern[Event, S, T],
  override val window: Window
) extends AccumPattern[Event, S, T, WindowStatisticResult, WindowStatisticAccumState[T]]:

  override def initialState(): AggregatorPState[S, T, WindowStatisticAccumState[T]] =
    AggregatorPState(
      innerState = inner.initialState(),
      innerQueue = PQueue.empty,
      astate = WindowStatisticAccumState(None, m.ArrayDeque.empty),
      indexTimeMap = m.ArrayDeque.empty
    )

case class WindowStatisticAccumState[T](
  lastValue: Option[WindowStatisticResult],
  windowQueue: m.ArrayDeque[WindowStatisticQueueInstance]
) extends AccumState[T, WindowStatisticResult, WindowStatisticAccumState[T]]:

  // val log = Logger[WindowStatisticAccumState[T]]

  override def updated(
    window: Window,
    times: m.ArrayDeque[(Idx, Time)],
    idxValue: IdxValue[T]
  ): (WindowStatisticAccumState[T], QI[WindowStatisticResult]) =
    val isSuccess = idxValue.value.isSuccess
    val (newLastValue, newWindowQueue, newOutputQueue) =
      times.foldLeft(Tuple3(lastValue, windowQueue, PQueue.empty[WindowStatisticResult])):
        case ((lastValue, windowQueue, outputQueue), (idx, time)) =>
          // log.warn(s"WinStats WQ length = ${windowQueue.length}, OQ length = ${outputQueue.size}")
          addOnePoint(time, idx, window, isSuccess, lastValue, windowQueue, outputQueue)

    WindowStatisticAccumState[T](newLastValue, newWindowQueue) -> newOutputQueue

  def addOnePoint(
    time: Time,
    idx: Idx,
    window: Window,
    isSuccess: Boolean,
    lastValue: Option[WindowStatisticResult],
    windowQueue: m.ArrayDeque[WindowStatisticQueueInstance],
    outputQueue: QI[WindowStatisticResult]
  ): (Option[WindowStatisticResult], m.ArrayDeque[WindowStatisticQueueInstance], QI[WindowStatisticResult]) =

    // log.warn(s"WinStats ${window.toMillis}, point arrived: $time, $idx, $isSuccess")
    // add new element to queue
    val (newLastValue, newWindowStatisticQueueInstance) =
      lastValue
        .map { cmr =>
          {

            val elem = WindowStatisticQueueInstance(
              idx = idx,
              time = time,
              isSuccess = isSuccess,
              // count success and fail times by previous result, not current!
              successTimeFromPrevious = if cmr.lastWasSuccess then time.toMillis - cmr.time.toMillis else 0,
              failTimeFromPrevious = if !cmr.lastWasSuccess then time.toMillis - cmr.time.toMillis else 0
            )

            val newLV = cmr.copy(time = time, lastWasSuccess = isSuccess).plusChange(elem, window)

            newLV -> elem
          }
        }
        .getOrElse(
          WindowStatisticResult(idx, time, isSuccess, if isSuccess then 1 else 0, 0, if !isSuccess then 1 else 0, 0)
            -> WindowStatisticQueueInstance(idx, time, isSuccess = isSuccess)
        )

    // remove outdated elements from queue
    val (outputs, updatedWindowQueue) = takeWhileFromQueue(windowQueue)(_.time.plus(window) < time)

    val finalNewLastValue = outputs.foldLeft(newLastValue) { case (cmr, elem) => cmr.minusChange(elem, window) }

    // we have to correct result because of the most early event in queue can contain additional time which is not in window.
    // val correctedLastValue = updatedWindowQueue.headOption
    //   .map { cmqi =>
    //     val maxChangeTime = window.toMillis - (finalNewLastValue.time.toMillis - cmqi.time.toMillis)
    //     val successCorrection =
    //       if (cmqi.successTimeFromPrevious == 0) 0 else cmqi.successTimeFromPrevious - maxChangeTime
    //     val failCorrection = if (cmqi.failTimeFromPrevious == 0) 0 else cmqi.failTimeFromPrevious - maxChangeTime
    //     finalNewLastValue.copy(
    //       successMillis = finalNewLastValue.successMillis - successCorrection,
    //       failMillis = finalNewLastValue.failMillis - failCorrection
    //     )
    //   }
    //   .getOrElse(finalNewLastValue)
    //   .copy(idx = idx)
    val correctedLastValue = finalNewLastValue.copy(idx = idx)

    // log.warn(s"WinStats, returned value: $idx, $correctedLastValue")

    val finalWindowQueue = { updatedWindowQueue.append(newWindowStatisticQueueInstance); updatedWindowQueue }
    val updatedOutputQueue = outputQueue.enqueue(IdxValue(idx, idx, Result.succ(correctedLastValue)))

    Tuple3(Some(correctedLastValue), finalWindowQueue, updatedOutputQueue)

case class WindowStatisticQueueInstance(
  idx: Idx,
  time: Time,
  isSuccess: Boolean,
  successTimeFromPrevious: Long = 0,
  failTimeFromPrevious: Long = 0
)

// OPTIMIZE memory, make successCount and failCount - Int
case class WindowStatisticResult(
  idx: Idx,
  time: Time,
  lastWasSuccess: Boolean,
  successCount: Long,
  successMillis: Long,
  failCount: Long,
  failMillis: Long
):
  def totalMillis: Long = successMillis + failMillis
  def totalCount: Long = successCount + failCount

  // val log = Logger[WindowStatisticResult]

  def plusChange(wsqi: WindowStatisticQueueInstance, window: Window): WindowStatisticResult =

    val newSuccessMillis = successMillis + math.min(wsqi.successTimeFromPrevious, window.toMillis - successMillis)
    val newFailMillis = failMillis + math.min(wsqi.failTimeFromPrevious, window.toMillis - failMillis)

    val successDecrease = if wsqi.isSuccess then 0 else Math.max(0, newSuccessMillis + newFailMillis - window.toMillis)
    val failDecrease = if !wsqi.isSuccess then 0 else Math.max(0, newSuccessMillis + newFailMillis - window.toMillis)

    val res = this.copy(
      successCount = successCount + (if wsqi.isSuccess then 1 else 0),
      successMillis = newSuccessMillis - successDecrease,
      failCount = failCount + (if !wsqi.isSuccess then 1 else 0),
      failMillis = newFailMillis - failDecrease
    )
    // log.warn(
    //  s"WinStats Result: plus change: this = $this, data = $wsqi, res = $res"
    // )
    res

  def minusChange(wsqi: WindowStatisticQueueInstance, window: Window): WindowStatisticResult =
    val pastTime = time.toMillis - wsqi.time.toMillis - window.toMillis
    val maxChangeTime = Math.min(window.toMillis, pastTime)

    // log.warn(
    //   s"WinStats Result: minus change: this = $this, data = $wsqi, past time = $pastTime, max change time = $maxChangeTime"
    // )

    if wsqi.isSuccess then
      this.copy(
        successCount = successCount - 1,
        successMillis = Math.max(0, successMillis - maxChangeTime),
        failCount = failCount,
        failMillis = failMillis
      )
    else
      this.copy(
        successCount = successCount,
        successMillis = successMillis,
        failCount = failCount - 1,
        failMillis = Math.max(0, failMillis - maxChangeTime)
      )
