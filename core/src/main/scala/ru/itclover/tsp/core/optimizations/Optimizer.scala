package ru.itclover.tsp.core.optimizations
import ru.itclover.tsp.core.Pattern.IdxExtractor
import ru.itclover.tsp.core.Pat
import ru.itclover.tsp.core._
import scala.language.existentials

import scala.language.reflectiveCalls
import ru.itclover.tsp.core.aggregators.GroupPattern
import ru.itclover.tsp.core.aggregators.PreviousValue
import ru.itclover.tsp.core.aggregators.TimerPattern
import ru.itclover.tsp.core.aggregators.WindowStatistic
import ru.itclover.tsp.core.io.TimeExtractor
import cats.kernel.Group
import scala.language.higherKinds

class Optimizer[E: IdxExtractor: TimeExtractor] {

  def optimizations[T] = Seq(optimizeInners[T], coupleOfTwoSimple[T], coupleOfTwoConst[T], mapOfConst[T], mapOfSimple[T])

  def optimizable[T](pat: Pat[E, T]): Boolean =
    optimizations[T].find(_.isDefinedAt(pat)).isDefined

  def applyOptimizations[T](pat: Pat[E, T]): (Pat[E, T], Boolean) = {
    optimizations[T].foldLeft(pat -> false) {
      case ((p, _), rule) if rule.isDefinedAt(p) => rule.apply(p).asInstanceOf[Pat[E, T]] -> true
      case (x, _)                                => x
    }
  }

  def optimize[T](pattern: Pat[E, T]): Pat[E, T] = {

    val optimizedPatternIterator = Iterator.iterate(pattern -> true) { case (p, _) => applyOptimizations(p) }

    // try no more than 10 cycles of optimizations to avoid infinite recursive loops in case of wrong rule.
    optimizedPatternIterator.takeWhile(_._2).take(10).map(_._1).toSeq.last
  }

  type OptimizeRule[T] = PartialFunction[Pat[E, T], Pat[E, T]]

  private def coupleOfTwoConst[T]: OptimizeRule[T] = {
    case Pat(x @ CouplePattern(Pat(ConstPattern(a)), Pat(ConstPattern(b)))) =>
      ConstPattern[E, T](x.func.apply(a, b))
  }

  private def coupleOfTwoSimple[T]: OptimizeRule[T] = {
    // couple(simple, simple) => simple
    case Pat(x @ CouplePattern(Pat(left @ SimplePattern(fleft)), Pat(right @ SimplePattern(fright)))) =>
      SimplePattern[E, T](
        event => x.func.apply(fleft.apply(event.asInstanceOf[Nothing]), fright.apply(event.asInstanceOf[Nothing]))
      )
    // couple(simple, const) => simple
    case Pat(x @ CouplePattern(Pat(left @ SimplePattern(fleft)), Pat(ConstPattern(r)))) =>
      SimplePattern[E, T](
        event => x.func.apply(fleft.apply(event.asInstanceOf[Nothing]), r)
      )
    // couple(const, simple) => simple
    case Pat(x @ CouplePattern(Pat(ConstPattern(l)), Pat(right @ SimplePattern(fright)))) =>
      SimplePattern[E, T](
        event => x.func.apply(l, fright.apply(event.asInstanceOf[Nothing]))
      )
    // couple(some, const) => map(some)
    case Pat(x @ CouplePattern(left, Pat(ConstPattern(r)))) =>
      MapPattern(forceState(left))(t => x.func.apply(Result.succ(t), r))
    // couple(const, some) => map(some)
    case Pat(x @ CouplePattern(Pat(ConstPattern(l)), right)) =>
      MapPattern(forceState(right))(t => x.func.apply(l, Result.succ(t)))
  }

  private def mapOfConst[T]: OptimizeRule[T] = {
    case Pat(map @ MapPattern(Pat(ConstPattern(x)))) => ConstPattern[E, T](x.flatMap(map.func))
  }

  private def mapOfSimple[T]: OptimizeRule[T] = {
    case Pat(map @ MapPattern(Pat(simple: SimplePattern[E, _]))) =>
      SimplePattern[E, T](simple.f.andThen(map.func))
  }

  private def optimizeInners[T]: OptimizeRule[T] = {
    case AndThenPattern(first, second) if optimizable(first) || optimizable(second) =>
      AndThenPattern(
        forceState(optimize(first)),
        forceState(optimize(second))
      )
    case x @ MapPattern(inner) if optimizable(inner) => MapPattern(forceState(optimize(inner)))(x.func)
    case x @ CouplePattern(left, right) if optimizable(left) || optimizable(right) =>
      CouplePattern(
        forceState(optimize(left)),
        forceState(optimize(right))
      )(x.func)
    case x: IdxMapPattern[E, _, _, _] if optimizable(x.inner) => new IdxMapPattern(forceState(x.inner))(x.func)
    case x: ReducePattern[E, _, _, _] if x.patterns.find(optimizable).isDefined => {
      def cast[S <: PState[T, S], T](pats: Seq[Pat[E, T]]): Seq[Pattern[E, S, T]] forSome { type S <: PState[T, S] } =
        pats.asInstanceOf[Seq[Pattern[E, S, T]]]
      new ReducePattern(cast(x.patterns.map(t => optimize(t))))(x.func, x.transform, x.filterCond, x.initial)
    }
    case x @ GroupPattern(inner, window) if optimizable(inner) => {
      implicit val group: Group[T] = x.group.asInstanceOf[Group[T]]
      val newInner: Pattern[E, S[T], T] = forceState(optimize(inner.asInstanceOf[Pat[E, T]]))
      GroupPattern[E, S[T], T](newInner, window).asInstanceOf[Pat[E, T]]
    }
    case PreviousValue(inner, window) if optimizable(inner)   => PreviousValue(forceState(optimize(inner)), window)
    case TimerPattern(inner, window) if optimizable(inner)    => TimerPattern(forceState(optimize(inner)), window)
    case WindowStatistic(inner, window) if optimizable(inner) => WindowStatistic(forceState(optimize(inner)), window)
  }

  type S[T] <: PState[T, S[T]]
  // Need to cast Pat[E,T] to some Pattern type. Pattern has restriction on State
  // type parameters which is constant, so simple asInstanceOf complains on
  // unmet restrictions.
  private def forceState[E, T](pat: Pat[E, T]): Pattern[E, S[T], T] =
    pat.asInstanceOf[Pattern[E, S[T], T]]

}
