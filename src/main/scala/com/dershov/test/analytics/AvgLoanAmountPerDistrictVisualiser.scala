package com.dershov.test.analytics

import org.apache.spark.sql.{DataFrame, SparkSession}

case class AvgLoanAmountPerDistrictVisualiser(implicit val spark:SparkSession) {
  def visualise(loans:DataFrame, accounts:DataFrame) : Unit = {
    import org.apache.spark.sql.functions._
    import vegas.sparkExt._
    import vegas._
    import vegas.render.WindowRenderer._

    
    val necessaryLoanData = loans.select(col("loanId"),
      col("amount"),
      col("accountId"))
    val dataFrame = accounts
      .join(necessaryLoanData, "accountId")
      .groupBy(col("districtId"))
      .agg(avg(col("amount")).as("avgLoanAmountPerDistrict"))


    val plot = Vegas("Average Loan Amount Per District Visualisation")
      .withDataFrame(dataFrame)
      .encodeX("districtId", Nom)
      .encodeY("avgLoanAmountPerDistrict", Quant)
      .mark(Bar)

    plot.show

  }
}
