package com.github.fortega

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import scala.util.Try

class TryUdfTest extends AnyFlatSpec {
  private val sparkServer = sys.env.getOrElse("SPARK_SERVER", "local")
  private val spark = SparkSession.builder.master(sparkServer).getOrCreate
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  "TryUdf" should "parse using String => Int" in {
    val positiveOrZero = TryUdf[String, Int] { (text: String) =>
      Try {
        val value = text.toInt
        if (value > 0)
          value
        else
          0
      }
    }

    val resultColumn = "result"
    val rawColumn = "raw"
    val raw = List(
      Int.MinValue.toString,
      "0",
      Int.MaxValue.toString,
      null,
      "💩",
      "i wish java support case classes",
    ).toDF(rawColumn)

    val data = raw
      .withColumn(resultColumn, positiveOrZero(col(rawColumn)))

    val succed = TryUdf.succed(data, resultColumn)
    val error = TryUdf.error(data, resultColumn)

    assert(succed.count == 3)
    assert(error.count == 3)
  }
}
