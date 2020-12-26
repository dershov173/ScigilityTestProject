package com.dershov.test.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window

case class MaxShareCalculator(implicit val spark:SparkSession) {
  def calculate(transactions: DataFrame,
                accounts: DataFrame): (Int, Double) = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    transactions
        .select("transId", "accountId", "operation")
        .join(accounts, "accountId")
        .groupBy("districtId", "operation")
        .agg(count("*").as("num_occurrences"))
        .withColumn("share", col("num_occurrences")
          / sum("num_occurrences").over(Window.partitionBy("districtId")))
        .where(col("operation") === lit("VYBER"))
        .select(col("districtId"), col("share"),
          max("share").over(Window.partitionBy()).as("max_share"))
        .where(col("share") === col("max_share"))
        .drop("share")
        .as[(Int, Double)]
        .first()
  }
}
