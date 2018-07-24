package ru.itclover.streammachine.newsyntax

import org.parboiled2._
import shapeless.HNil

sealed trait Expr
sealed trait ArithmeticExpr extends Expr
sealed trait BooleanExpr extends Expr
sealed trait TrileanExpr extends Expr
sealed trait RangeExpr extends Expr

object Operators {

  sealed abstract class Value(op: String) {
    val operatorSymbol: String = op

    def comp[T](implicit num: Fractional[T]): (T, T) => T
  }

  case object Add extends Value("+") {
    override def comp[T](implicit num: Fractional[T]): (T, T) => T = num.plus
  }

  case object Sub extends Value("-") {
    override def comp[T](implicit num: Fractional[T]): (T, T) => T = num.minus
  }

  case object Mul extends Value("*") {
    override def comp[T](implicit num: Fractional[T]): (T, T) => T = num.times
  }

  case object Div extends Value("/") {
    override def comp[T](implicit num: Fractional[T]): (T, T) => T = num.div
  }

}

object ComparisonOperators {

  sealed abstract class Value(op: String) {
    def comparingFunction[T](implicit ord: Ordering[T]): (T, T) => Boolean

    val operatorSymbol: String = op
  }

  case object Equal extends Value("==") {
    override def comparingFunction[T](implicit ord: Ordering[T]): (T, T) => Boolean = ord.equiv
  }

  case object NotEqual extends Value("!=") {
    override def comparingFunction[T](implicit ord: Ordering[T]): (T, T) => Boolean = (x1, x2) => !ord.equiv(x1, x2)
  }

  case object Less extends Value("<") {
    override def comparingFunction[T](implicit ord: Ordering[T]): (T, T) => Boolean = ord.lt
  }

  case object Greater extends Value(">") {
    override def comparingFunction[T](implicit ord: Ordering[T]): (T, T) => Boolean = ord.gt
  }

  case object LessOrEqual extends Value("<=") {
    override def comparingFunction[T](implicit ord: Ordering[T]): (T, T) => Boolean = ord.lteq
  }

  case object GreaterOrEqual extends Value(">=") {
    override def comparingFunction[T](implicit ord: Ordering[T]): (T, T) => Boolean = ord.gteq
  }

}

object BooleanOperators {

  sealed abstract class Value(op: String) {
    val operatorSymbol: String = op

    def comparingFunction: (Boolean, Boolean) => Boolean
  }

  case object And extends Value("and") {
    override def comparingFunction: (Boolean, Boolean) => Boolean = (x1, x2) => x1 & x2
  }

  case object Or extends Value("or") {
    override def comparingFunction: (Boolean, Boolean) => Boolean = (x1, x2) => x1 | x2
  }

  case object Not extends Value("not") {
    override def comparingFunction: (Boolean, Boolean) => Boolean = (x1, _) => !x1
  }

}

object TrileanOperators {

  sealed trait Value

  case object And extends Value

  case object AndThen extends Value

  case object Or extends Value

}

// TODO: storing time-related attributes
final case class TrileanCondExpr(cond: Expr, exactly: Boolean = false,
                                 window: TimeLiteral = null, range: Expr = null, until: Expr = null) extends TrileanExpr

final case class TrileanOnlyBooleanExpr(cond: BooleanExpr) extends TrileanExpr

final case class FunctionCallExpr(fun: String, args: List[Expr]) extends ArithmeticExpr

final case class ComparisonOperatorExpr(op: ComparisonOperators.Value, lhs: ArithmeticExpr, rhs: ArithmeticExpr) extends BooleanExpr

final case class BooleanOperatorExpr(op: BooleanOperators.Value, lhs: BooleanExpr, rhs: BooleanExpr) extends BooleanExpr

final case class TrileanOperatorExpr(op: TrileanOperators.Value, lhs: TrileanExpr, rhs: TrileanExpr) extends TrileanExpr

final case class OperatorExpr(op: Operators.Value, lhs: ArithmeticExpr, rhs: ArithmeticExpr) extends ArithmeticExpr

final case class TimeRangeExpr(lower: TimeLiteral, upper: TimeLiteral, strict: Boolean) extends RangeExpr {
  def contains(x: Long): Boolean = if (strict) {
    (lower == null || x > lower.millis) && (upper == null || x < upper.millis)
  } else {
    (lower == null || x >= lower.millis) && (upper == null || x <= upper.millis)
  }
}

final case class RepetitionRangeExpr(lower: IntegerLiteral, upper: IntegerLiteral, strict: Boolean) extends RangeExpr {
  def contains(x: Long): Boolean = if (strict) {
    (lower == null || x > lower.value) && (upper == null || x < upper.value)
  } else {
    (lower == null || x >= lower.value) && (upper == null || x <= upper.value)
  }
}

final case class Identifier(identifier: String) extends ArithmeticExpr

final case class IntegerLiteral(value: Long) extends ArithmeticExpr

final case class TimeLiteral(millis: Long) extends Expr

final case class DoubleLiteral(value: Double) extends ArithmeticExpr

final case class StringLiteral(value: String) extends ArithmeticExpr

final case class BooleanLiteral(value: Boolean) extends BooleanExpr


class SyntaxParser(val input: ParserInput) extends Parser {

  def start: Rule1[TrileanExpr] = rule {
    trileanExpr ~ EOI
  }

  def trileanExpr: Rule1[TrileanExpr] = rule {
    trileanTerm ~ zeroOrMore(
      ignoreCase("andthen") ~ ws ~ trileanTerm ~>
        ((e: TrileanExpr, f: TrileanExpr) => TrileanOperatorExpr(TrileanOperators.AndThen, e, f))
        |
        ignoreCase("and") ~ ws ~ trileanTerm ~> ((e: TrileanExpr, f: TrileanExpr) => TrileanOperatorExpr(TrileanOperators.And, e, f))
        | ignoreCase("or") ~ ws ~ trileanTerm ~> ((e: TrileanExpr, f: TrileanExpr) => TrileanOperatorExpr(TrileanOperators.Or, e, f))
    )
  }

  def trileanTerm: Rule1[TrileanExpr] = rule {
    (trileanFactor ~ ignoreCase("for") ~ ws ~ optional(ignoreCase("exactly") ~ ws ~> (() => 1)) ~ time ~ optional(range) ~>
      ((c: Expr, ex: Option[Int], w: TimeLiteral, r: Option[Expr])
      => TrileanCondExpr(c, exactly = ex.isDefined, window = w, range = r.orNull))
      | trileanFactor ~ ignoreCase("until") ~ ws ~ booleanExpr ~ optional(range) ~>
      ((c: Expr, b: Expr, r: Option[Expr]) => TrileanCondExpr(c, until = b, range = r.orNull))
      | trileanFactor
      )
  }

  def trileanFactor: Rule1[TrileanExpr] = rule {
    booleanExpr ~> { b: BooleanExpr => TrileanOnlyBooleanExpr(b) } | '(' ~ trileanExpr ~ ')' ~ ws
  }

  def booleanExpr: Rule1[BooleanExpr] = rule {
    booleanTerm ~ zeroOrMore(
      ignoreCase("or") ~ ws ~ booleanTerm ~> ((e: BooleanExpr, f: BooleanExpr) => BooleanOperatorExpr(BooleanOperators.Or, e, f)))
  }

  def booleanTerm: Rule1[BooleanExpr] = rule {
    booleanFactor ~ zeroOrMore(
      ignoreCase("and") ~ ws ~ booleanFactor ~> ((e: BooleanExpr, f: BooleanExpr) => BooleanOperatorExpr(BooleanOperators.And, e, f)))
  }

  def booleanFactor: Rule1[BooleanExpr] = rule {
    (comparison | boolean | "(" ~ booleanExpr ~ ")" ~ ws
      | "not" ~ booleanExpr ~> ((b: BooleanExpr) => BooleanOperatorExpr(BooleanOperators.Not, b, null)))
  }

  def comparison: Rule1[BooleanExpr] = rule {
    (
      expr ~ "<" ~ ws ~ expr ~> ((e1: ArithmeticExpr, e2: ArithmeticExpr) =>
        ComparisonOperatorExpr(ComparisonOperators.Less, e1, e2))
        | expr ~ "<=" ~ ws ~ expr ~> ((e1: ArithmeticExpr, e2: ArithmeticExpr) =>
        ComparisonOperatorExpr(ComparisonOperators.LessOrEqual, e1, e2))
        | expr ~ ">" ~ ws ~ expr ~> ((e1: ArithmeticExpr, e2: ArithmeticExpr) =>
        ComparisonOperatorExpr(ComparisonOperators.Greater, e1, e2))
        | expr ~ ">=" ~ ws ~ expr ~> ((e1: ArithmeticExpr, e2: ArithmeticExpr) =>
        ComparisonOperatorExpr(ComparisonOperators.GreaterOrEqual, e1, e2))
        | expr ~ "=" ~ ws ~ expr ~> ((e1: ArithmeticExpr, e2: ArithmeticExpr) =>
        ComparisonOperatorExpr(ComparisonOperators.Equal, e1, e2))
        | expr ~ ("!=" | "<>") ~ ws ~ expr ~> ((e1: ArithmeticExpr, e2: ArithmeticExpr) =>
        ComparisonOperatorExpr(ComparisonOperators.NotEqual, e1, e2))
      )
  }

  def expr: Rule1[ArithmeticExpr] = rule {
    term ~ zeroOrMore('+' ~ ws ~ term ~> ((e: ArithmeticExpr, f: ArithmeticExpr) => OperatorExpr(Operators.Add, e, f))
      | '-' ~ ws ~ term ~> ((e: ArithmeticExpr, f: ArithmeticExpr) => OperatorExpr(Operators.Sub, e, f))
    )
  }

  def term: Rule1[ArithmeticExpr] = rule {
    factor ~ zeroOrMore('*' ~ ws ~ factor ~> ((e: ArithmeticExpr, f: ArithmeticExpr) => OperatorExpr(Operators.Mul, e, f))
      | '/' ~ ws ~ factor ~> ((e: ArithmeticExpr, f: ArithmeticExpr) => OperatorExpr(Operators.Div, e, f))
    )
  }

  def factor: Rule1[ArithmeticExpr] = rule {
    real | integer | string | functionCall | identifier | '(' ~ expr ~ ')' ~ ws
  }

  def range: Rule1[RangeExpr] = rule {
    timeRange | repetitionRange
  }

  def timeRange: Rule1[RangeExpr] = rule {
    ("<" ~ ws ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(null, t, strict = true))
      | "<=" ~ ws ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(null, t, strict = false))
      | ">" ~ ws ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(t, null, strict = true))
      | ">=" ~ ws ~ time ~> ((t: TimeLiteral) => TimeRangeExpr(t, null, strict = false))
      | time ~ ignoreCase("to") ~ ws ~ time ~>
      ((t1: TimeLiteral, t2: TimeLiteral) => TimeRangeExpr(t1, t2, strict = false))
      | real ~ ignoreCase("to") ~ ws ~ real ~ timeUnit ~> ((d1: DoubleLiteral, d2: DoubleLiteral, u: Int) =>
      TimeRangeExpr(TimeLiteral((d1.value * u).toLong), TimeLiteral((d2.value * u).toLong), strict = false))
      )
  }

  def repetitionRange: Rule1[RangeExpr] = rule {
    ("<" ~ ws ~ repetition ~> ((t: IntegerLiteral) => RepetitionRangeExpr(null, t, strict = true))
      | "<=" ~ ws ~ repetition ~> ((t: IntegerLiteral) => RepetitionRangeExpr(null, t, strict = false))
      | ">" ~ ws ~ repetition ~> ((t: IntegerLiteral) => RepetitionRangeExpr(t, null, strict = true))
      | ">=" ~ ws ~ repetition ~> ((t: IntegerLiteral) => RepetitionRangeExpr(t, null, strict = false))
      | integer ~ ignoreCase("to") ~ ws ~ repetition ~>
      ((t1: IntegerLiteral, t2: IntegerLiteral) => RepetitionRangeExpr(t1, t2, strict = false))
      )
  }

  def repetition: Rule1[IntegerLiteral] = rule {
    integer ~ ignoreCase("times")
  }

  def time: Rule1[TimeLiteral] = rule {
    singleTime.+(ws) ~> ((ts: Seq[TimeLiteral]) => TimeLiteral(ts.foldLeft(0L) { (acc, t) => acc + t.millis }))
  }

  def singleTime: Rule1[TimeLiteral] = rule {
    real ~ timeUnit ~ ws ~>
      ((i: DoubleLiteral, u: Int) => TimeLiteral((i.value * u).toLong))
  }

  def timeUnit: Rule1[Int] = rule {
    (ignoreCase("seconds") ~> (() => 1000)
      | ignoreCase("sec") ~> (() => 1000)
      | ignoreCase("minutes") ~> (() => 60000)
      | ignoreCase("min") ~> (() => 60000)
      | ignoreCase("milliseconds") ~> (() => 1)
      | ignoreCase("ms") ~> (() => 1)
      | ignoreCase("hours") ~> (() => 3600000)
      | ignoreCase("hr") ~> (() => 3600000))
  }

  def real: Rule1[DoubleLiteral] = rule {
    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1)) ~
      capture(oneOrMore(CharPredicate.Digit) ~ optional('.' ~ oneOrMore(CharPredicate.Digit))) ~ ws
      ~> ((s: Int, i: String) => DoubleLiteral(s * i.toDouble))
      )
  }

  def integer: Rule1[IntegerLiteral] = rule {
    ((str("+") ~> (() => 1) | str("-") ~> (() => -1) | str("") ~> (() => 1))
      ~ capture(oneOrMore(CharPredicate.Digit)) ~ ws
      ~> ((s: Int, i: String) => IntegerLiteral(s * i.toInt))
      )
  }

  def functionCall: Rule1[FunctionCallExpr] = rule {
    identifier ~ ws ~ "(" ~ ws ~ (time | expr).*(ws ~ "," ~ ws) ~ ")" ~ ws ~> ((i: Identifier, el: Seq[Expr]) =>
      FunctionCallExpr(i.identifier.toLowerCase, el.toList))
  }

  def identifier: Rule1[Identifier] = rule {
    (capture(CharPredicate.Alpha ~ zeroOrMore(CharPredicate.AlphaNum | '_')) ~ ws ~> ((id: String) => Identifier(id))
      | '"' ~ capture(oneOrMore(noneOf("\"") | "\"\"")) ~ '"' ~ ws ~>
      ((id: String) => Identifier(id.replace("\"\"", "\"")))
      )
  }

  def string: Rule1[StringLiteral] = rule {
    "'" ~ capture(oneOrMore(noneOf("'") | "''")) ~ "'" ~ ws ~> ((id: String) => StringLiteral(id.replace("''", "'")))
  }

  def boolean: Rule1[BooleanLiteral] = rule {
    (ignoreCase("true") ~ ws ~> (() => BooleanLiteral(true))
      | ignoreCase("false") ~ ws ~> (() => BooleanLiteral(false)) ~ ws)
  }

  def ws = rule {
    quiet(zeroOrMore(anyOf(" \t \n")))
  }
}