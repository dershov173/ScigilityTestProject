package com.dershov.test.checks

import com.dershov.test.DataQualityCheck
import org.apache.spark.sql.DataFrame

case class TransIdDuplicatesFinder(rawTransTable:DataFrame) extends DataQualityCheck{
  override def findCorruptedRecords(): DataFrame = {
    import org.apache.spark.sql.functions._
    rawTransTable
      .groupBy(col("transId"))
      .agg(count("*").as("num_occurrences"))
      .where(col("num_occurrences") > 1)
  }
}
