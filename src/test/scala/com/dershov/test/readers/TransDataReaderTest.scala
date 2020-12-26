package com.dershov.test.readers

import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

class TransDataReaderTest extends FlatSpec with Matchers {

  implicit val session: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("CSV_Reader_Test")
    .getOrCreate()
  session

    "TransDataFrameReader" should "properly read a csv file" in {
      val url = this.getClass.getResource("trans_sample.csv")

      TransDataReader.readDataFrameFromCSV(url.toString)
        .count() should equal(3)
    }

//  "do" should "do  something " in {
//    import functions._
//    val transDataURL = this.getClass.getResource("trans.csv")
//    val accountIdFromTransTable = TransDataReader.readDataFrameFromCSV(transDataURL.toString)
//      .select("accountId")
//      .distinct()
//      .cache()
////    println(accountIdFromTransTable.count())
////    accountIdFromTransTable
////      .show()
//
//    val accountDataURL = this.getClass.getResource("account.csv")
//    val accountsFromAccountsTable = AccountDataReader.readDataFrameFromCSV(accountDataURL.toString)
//      .select("accountId")
//      .distinct()
//      .cache()
////    println(accountsFromAccountsTable.count())
//
//    accountIdFromTransTable.except(accountsFromAccountsTable).show()
//
//  }

//  "transid" should "find duplicates" in {
//    import functions._
//    val transDataURL = this.getClass.getResource("trans.csv")
//    val accountIdFromTransTable = TransDataReader.readDataFrameFromCSV(transDataURL.toString)
//      .select("*")
//
//    accountIdFromTransTable
//        .select("*")
//        .where(col("transId") === lit(685100))
//      .show()
//
////    accountIdFromTransTable
////      .groupBy("transId")
////      .agg(count("*").as("num_occurrences"))
////      .where(col("num_occurrences") > 1)
////      .show()
//  }

}
