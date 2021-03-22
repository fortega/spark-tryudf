package com.github.fortega

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Try, Success, Failure}

case class Result[A, B](value: Option[B], exception: String, origin: A)

object TryUdf {
  def apply[A: TypeTag, B: TypeTag](
      f: A => Try[B]
  ): UserDefinedFunction = udf[Result[A, B], A] { origin =>
    f(origin) match {
      case Failure(e) =>
        Result(
          None,
          s"${e.getClass.getName}: ${e.getMessage}",
          origin
        )
      case Success(value) => Result(Some(value), null, origin)
    }
  }
}
