package com.github.fortega

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.UserDefinedFunction
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Try, Success,Failure}

case class Result[A](value: A, exception: String)

object TryUdf {
  def apply[A: TypeTag](
      f: A => Try[A]
  ): UserDefinedFunction = udf[Result[A], A] { value =>
    f(value) match {
      case Failure(e) =>
        Result(value, s"${e.getClass.getName}: ${e.getMessage}")
      case Success(value) => Result(value, null)
    }
  }
}
