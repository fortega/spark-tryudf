package com.github.fortega

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Try, Success, Failure}

case class Result[A, B](value: Option[B], exception: String, origin: A)

object TryUdf {

  /** Create spark udf for function that return Try
    *
    * @param f function
    * @return column with [[com.github.fortega.Result]] struct (value, exception and origin)
    */
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

  /** Return dataframe with success execution
    * @param df dataframe with function execution
    * @param columnName column name of the function execution
    * @return success execution dataframe
    */
  def success(df: DataFrame, columnName: String) =
    df.filter(col(columnName)("exception") isNull)

  /** Return dataframe with failure execution
    * @param df dataframe with function execution
    * @param columnName column name of the function execution
    * @return failure execution dataframe
    */
  def failure(df: DataFrame, columnName: String) =
    df.filter(col(columnName)("exception") isNotNull)
}
