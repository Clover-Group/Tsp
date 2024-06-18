package ru.itclover.tsp.core.io

trait BasicDecoders[From] {
  implicit def decodeToDouble: Decoder[From, Double]
  implicit def decodeToInt: Decoder[From, Int]
  implicit def decodeToString: Decoder[From, String]
  implicit def decodeToAny: Decoder[From, Any]
}

// Here we deal with data coming from non-Scala (probably Javaesque) sources,
// so we deliberately use Any and null there
@SuppressWarnings(Array("org.wartremover.warts.Any", "org.wartremover.warts.Null"))
object AnyDecodersInstances extends BasicDecoders[Any] with Serializable {
  import Decoder._

  implicit val decodeToDouble: Decoder[Any, Double] = new AnyDecoder[Double] {

    override def apply(x: Any): Double = x match {
      case d: Double           => d
      case n: java.lang.Number => n.doubleValue()
      case s: String =>
        try {
          Helper.strToDouble(s)
        } catch {
          case _: Exception =>
            // throw new RuntimeException(s"Cannot parse String ($s) to Double, exception: ${e.toString}")
            Double.NaN
        }
      case null => Double.NaN
    }

  }

  implicit val decodeToInt: Decoder[Any, Int] = new AnyDecoder[Int] {

    override def apply(x: Any): Int = x match {
      case i: Int              => i
      case n: java.lang.Number => n.intValue()
      case s: String =>
        try {
          Helper.strToInt(s)
        } catch {
          case e: Exception => sys.error(s"Cannot parse String ($s) to Int, exception: ${e.toString}")
        }
      case null => sys.error(s"Cannot parse null to Int")
    }

  }

  implicit val decodeToLong: Decoder[Any, Long] = new AnyDecoder[Long] {

    override def apply(x: Any): Long = x match {
      case i: Int              => i.toLong
      case l: Long             => l
      case n: java.lang.Number => n.longValue()
      case s: String =>
        try {
          Helper.strToLong(s)
        } catch {
          case e: Exception => sys.error(s"Cannot parse String ($s) to Long, exception: ${e.toString}")
        }
      case null => sys.error(s"Cannot parse null to Long")
    }

  }

  implicit val decodeToBoolean: Decoder[Any, Boolean] = new AnyDecoder[Boolean] {

    override def apply(x: Any): Boolean = x match {
      // case 0 | 0L | 0.0 | "0" | "false" | "off" | "no" => false
      // case 1 | 1L | 1.0 | "1" | "true" | "on" | "yes"  => true
      case i: Int              => i != 0
      case l: Long             => l != 0L
      case d: Double           => d != 0.0 && !d.isNaN
      case n: java.lang.Number => n.doubleValue() != 0.0
      case s: String =>
        s.toLowerCase() match {
          case "0" | "no" | "false" | "off" => false
          case "1" | "yes" | "true" | "on"  => true
          case _                            => sys.error(s"Cannot parse '$x' to Boolean")
        }
      case b: Boolean => b
      case null       => sys.error(s"Cannot parse null to Boolean")
      case _          => sys.error(s"Cannot parse '$x' to Boolean")
    }

  }

  implicit val decodeToString: Decoder[Any, String] = Decoder { (x: Any) =>
    if (x != null) x.toString else ""
  }

  implicit val decodeToAny: Decoder[Any, Any] = Decoder { (x: Any) =>
    x
  }

//  implicit val decodeToNull: Decoder[Any, Null] = Decoder { x: Any =>
//    null
//  }
}

@SuppressWarnings(Array("org.wartremover.warts.Any"))
object DoubleDecoderInstances extends BasicDecoders[Double] {

  implicit override def decodeToDouble: Decoder[Double, Double] = Decoder { (d: Double) =>
    d
  }

  implicit override def decodeToInt: Decoder[Double, Int] = Decoder { (d: Double) =>
    d.toInt
  }

  implicit override def decodeToString: Decoder[Double, String] = Decoder { (d: Double) =>
    d.toString
  }

  implicit override def decodeToAny: Decoder[Double, Any] = Decoder { (d: Double) =>
    d
  }

}

// Hack for String.toInt implicit method
object Helper {
  def strToInt(s: String): Int = s.toInt
  def strToLong(s: String): Long = s.toLong
  def strToDouble(s: String): Double = s.toDouble
}
