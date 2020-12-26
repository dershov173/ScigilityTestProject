package com.dershov.test.checks

import com.dershov.test.DataQualityCheck
import org.apache.spark.sql.{DataFrame, SparkSession}

case class SuspiciousAccountIdsFinder(rawTransDF: DataFrame,
                                      rawAccountDF: DataFrame)
                                     (implicit val spark: SparkSession)
  extends DataQualityCheck {
  override def findCorruptedRecords(): DataFrame = {
    import org.apache.spark.sql.functions._
    val accountIdFromTransTable = rawTransDF
      .select("accountId")
      .distinct()

    val accountsFromAccountsTable = rawAccountDF
      .select("accountId")

    broadcast(accountIdFromTransTable
      .except(accountsFromAccountsTable)
      .select(col("accountId").as("suspiciousAccountId")))
  }
}
