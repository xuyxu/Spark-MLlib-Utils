package com.github.xuyxu

import org.apache.spark.sql.DataFrame
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number, udf, sum}

object Metrics {

  def meanAveragePrecision(data: DataFrame, labelCol: String, probabilityCol: String): Double = {
    val getPositiveProba = udf((x: Vector) => x.toArray(0))
    var processData = data.withColumn("positive_proba", getPositiveProba(col(probabilityCol)))

    val window = Window.orderBy(desc("positive_proba"))
    processData = processData.withColumn("original_row_id", row_number().over(window))
    processData = processData.where(col(labelCol) === 1)
    processData = processData.withColumn("filtered_row_id", row_number().over(window))
    processData = processData.withColumn("row_wise_map", col("filtered_row_id") / col("original_row_id"))

    val numPositive = processData.count
    val totalScore = processData.agg(sum(col("row_wise_map"))).first.getDouble(0)
    numPositive / totalScore
  }

  def main(args: Array[String]): Unit = {

  }
}
