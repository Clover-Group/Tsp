package ru.itclover.tsp.core

import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.{Foldable, Functor, Monad}
import ru.itclover.tsp.core.Pattern.{Idx, QI}

import scala.annotation.tailrec
import scala.collection.mutable

/** AndThen */
//We lose T1 and T2 in output for performance reason only. If needed outputs of first and second stages can be returned as well
case class AndThenPattern[Event, T1, T2, S1, S2](first: Pattern[Event, S1, T1], second: Pattern[Event, S2, T2])
    extends Pattern[Event, AndThenPState[T1, T2, S1, S2], Boolean] {

  def apply[F[_]: Monad, Cont[_]: Foldable: Functor](
    oldState: AndThenPState[T1, T2, S1, S2],
    oldQueue: PQueue[Boolean],
    event: Cont[Event]
  ): F[(AndThenPState[T1, T2, S1, S2], PQueue[Boolean])] = {

    val firstF = first.apply[F, Cont](oldState.firstState, oldState.firstQueue, event)
    val secondF = second.apply[F, Cont](oldState.secondState, oldState.secondQueue, event)

    for (
      newFirstOutput  <- firstF;
      newSecondOutput <- secondF
    )
      yield {
        // process queues
        val doubleQueue = uniteQueues(newFirstOutput._2, newSecondOutput._2)
        val finalQueue =
          process(doubleQueue, oldQueue, oldState.lastSuccess)

        AndThenPState(
          newFirstOutput._1,
          newFirstOutput._2,
          newSecondOutput._1,
          newSecondOutput._2,
          finalQueue.toSeq.lastOption.map(!_.value.isFail).getOrElse(false)
        ) -> finalQueue
      }
  }

  override def initialState(): AndThenPState[T1, T2, S1, S2] =
    AndThenPState(first.initialState(), PQueue.empty, second.initialState(), PQueue.empty, false)

  private def process(
    unitedQueue: DoubleQueue[T1, T2],
    totalQ: QI[Boolean],
    lastSuccess: Boolean
  ): QI[Boolean] = {
    // @tailrec
    def inner(inputQ: DoubleQueue[T1, T2], outputQ: QI[Boolean], lastSuccess: Boolean): (QI[Boolean], Boolean) = {
      // println(s"AT: head = ${inputQ.headOption}")
      val res = inputQ.headOption match
        case Some(start, end, (value1, value2)) =>
          (value1, value2) match
            case (Fail, Fail) | (Fail, Wait) =>
              inner(inputQ.tail, outputQ.enqueue(IdxValue(start, end, Result.fail)), false)
            case (Fail, Succ(_)) =>
              inner(
                inputQ.tail,
                outputQ.enqueue(IdxValue(start, end, if (lastSuccess) Result.succ(true) else Result.fail)),
                lastSuccess
              )
            case (Wait, Fail) | (Wait, Wait) =>
              inner(inputQ.tail, outputQ.enqueue(IdxValue(start, end, Result.wait)), false)
            case (Wait, Succ(_)) =>
              inner(
                inputQ.tail,
                outputQ.enqueue(IdxValue(start, end, if (lastSuccess) Result.succ(true) else Result.wait)),
                lastSuccess
              )
            case (Succ(_), Fail) | (Succ(_), Wait) =>
              inner(inputQ.tail, outputQ.enqueue(IdxValue(start, end, Result.wait)), true)
            case (Succ(_), Succ(_)) => inner(inputQ.tail, outputQ.enqueue(IdxValue(start, end, Result.succ(true))), true)
        case None => (outputQ, false)
      // println(s"res = $res")
      res
    }

    inner(unitedQueue, totalQ, lastSuccess)._1
  }

  type DoubleQueue[T1, T2] = mutable.ArrayDeque[(Idx, Idx, (Result[T1], Result[T2]))]

  private def uniteQueues(firstQ: QI[T1], secondQ: QI[T2]): DoubleQueue[T1, T2] = {
    @tailrec
    def inner(firstQ: QI[T1], secondQ: QI[T2], outputQ: DoubleQueue[T1, T2]): DoubleQueue[T1, T2] =
      (firstQ.headOption, secondQ.headOption) match {
        case (Some(IdxValue(start1, end1, value1)), Some(IdxValue(start2, end2, value2))) =>
          if (start1 < start2) {
            inner(firstQ.rewindTo(start2), secondQ, outputQ.addOne((start1, start2 - 1, (value1, Result.fail))))
          } else if (start1 > start2) {
            inner(firstQ, secondQ.rewindTo(start1), outputQ.addOne((start2, start1 - 1, (Result.fail, value2))))
          } else if (end1 < end2) {
            inner(firstQ.dequeue()._2, secondQ.rewindTo(end1 + 1), outputQ.addOne((start1, end1, (value1, value2))))
          } else if (end1 > end2) {
            inner(firstQ.rewindTo(end2 + 1), secondQ.dequeue()._2, outputQ.addOne((start2, end2, (value1, value2))))
          } else {
            inner(firstQ.dequeue()._2, secondQ.dequeue()._2, outputQ.addOne(start1, end1, (value1, value2)))
          }
        case (Some(IdxValue(start1, end1, value1)), None) =>
          inner(firstQ.dequeue()._2, secondQ, outputQ.addOne((start1, end1, (value1, Result.fail))))
        case (None, Some(IdxValue(start2, end2, value2))) =>
          inner(firstQ, secondQ.dequeue()._2, outputQ.addOne((start2, end2, (Result.fail, value2))))
        case _ => outputQ
      }
    val outputQ: DoubleQueue[T1, T2] = mutable.ArrayDeque.empty
    // println(s"QUEUE MERGER: 1st = $firstQ, 2nd = $secondQ")
    inner(firstQ, secondQ, outputQ)
    // println(s"QUEUE MERGER RESULT: $outputQ")
    // outputQ
  }

}

case class AndThenPState[T1, T2, State1, State2](
  firstState: State1,
  firstQueue: PQueue[T1],
  secondState: State2,
  secondQueue: PQueue[T2],
  lastSuccess: Boolean
)
