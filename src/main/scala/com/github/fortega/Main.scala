package com.github.fortega

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import scala.util.{Try, Failure, Success}

object Main extends App {
  val positiveOrZero = TryUdf[String, Int] { (text: String) =>
    Try {
      val value = text.toInt
      if (value > 0)
        value
      else
        0
    }
  }

  val spark = SparkSession.builder.master("local[*]").getOrCreate
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  println("raw")
  val raw = List(
    "1",
    "432",
    "-4",
    "424",
    null,
    "40",
    "💩",
    " ",
    "4,5",
    "12.145.564-8"
  ).toDF("raw")
  raw.show

  println("data")
  val data = raw
    .withColumn("result", positiveOrZero($"raw"))
  data.show(false)
  data.printSchema

  println("succed")
  val succed = data
    .filter($"result" ("exception").isNull)
    .withColumn("result", $"result" ("value"))
  succed.show(false)
  succed.explain()

  println("error")
  val error = data
    .filter($"result" ("exception").isNotNull)
    .select($"raw", $"result" ("exception") as "exception")
  error.show(false)
  error.explain()

  val numTotal = data.count
  val numError = error.count
  spark.stop()

  println(
    s"readed: $numTotal | error: $numError (${numError * 100.0 / numTotal}%)"
  )
  val exitCode = if (numError == 0) 0 else 1
  sys.exit(exitCode)
}
