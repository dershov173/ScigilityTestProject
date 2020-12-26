package com.dershov.test.analytics

import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CorrelationFinder(implicit val spark: SparkSession) {
  def findCorrelation(transactions: DataFrame,
                      accounts: DataFrame,
                      districts: DataFrame): Double = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val dfToComputeCorr: DataFrame = transactions
      .select("transId", "accountId", "operation")
      .where(col("operation") === lit("VYBER"))
      .join(accounts, "accountId")
      .groupBy("districtId")
      .agg(count("*").as("numOfWithdrawalsInCash"))
      .join(districts, "districtId")


    val numOfInhabits: RDD[Double] = dfToComputeCorr
      .select("numOfInhabits")
      .orderBy("numOfInhabits")
      .as[Double]
      .rdd

    val numOfWithdrawalsInCash: RDD[Double] = dfToComputeCorr
      .select("numOfWithdrawalsInCash")
      .orderBy("numOfWithdrawalsInCash")
      .as[Double]
      .rdd

    Statistics.corr(numOfInhabits, numOfWithdrawalsInCash, "spearman")
  }
}
