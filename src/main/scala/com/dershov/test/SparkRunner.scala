package com.dershov.test

import com.dershov.test.analytics.{AvgLoanAmountPerDistrictVisualiser, CorrelationFinder, MaxShareCalculator}
import com.dershov.test.checks.TransIdDuplicatesFinder
import com.dershov.test.readers.{AccountDataReader, DistrictDataReader, LoanDataReader, TransDataReader}
import org.apache.spark.ml.stat.{ChiSquareTest, Correlation}
import org.apache.spark.ml.linalg.{Matrix, Vectors}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, Row, SparkSession}


object SparkRunner extends App {
  private implicit val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("Scigility_test_engine")
    .config("spark.sql.shuffle.partitions", 10)
    .getOrCreate()

  import org.apache.spark.sql.functions._

  private val cleanedDF: DataFrame =
    new AccountIdsExtractCheckRepairPipeline()
      .performExtractCheckRepairPipeline()
      .drop(col("branch"))
//
  val pathToAccountTable = this.getClass.getResource("account.csv").toString
  private val accountData: DataFrame =
    broadcast(AccountDataReader.readDataFrameFromCSV(pathToAccountTable)
        .select("accountId", "districtId"))

//  println(MaxShareCalculator().calculate(cleanedDF, accountData))



  private val pathToDistrictData: String = this.getClass.getResource("district.csv").toString
  private val districtData: DataFrame = DistrictDataReader.readDataFrameFromCSV(pathToDistrictData)
    .select(col("districtId"),
      col("A4").as("numOfInhabits"))

//  println(CorrelationFinder().findCorrelation(cleanedDF, accountData, districtData))

  private val pathToLoansData: String = this.getClass.getResource("loan.csv").toString
  private val loans: DataFrame = LoanDataReader.readDataFrameFromCSV(pathToLoansData)

  AvgLoanAmountPerDistrictVisualiser().visualise(loans, accountData)

  //Faker library in Python
  //https://databricks.com/blog/2017/02/13/anonymizing-datasets-at-scale-leveraging-databricks-interoperability.html
  //Our column can have repeatable records
  //1) monotonically_increasing_id() function of Spark. Data Integrity suffers
  //2) Map old keys to newly generated keys, join table with old keys to
  // the table with mappings by oldKey id, remove old key column. Performance ?
  //3) Map the column to sha1 . Data Integrity would be ok, performance as well
  //4) substr, concat, etc. Performance +, but data integrity - and security -


//  cleanedDF.join(accountData,
//    col("accountId") === col("account_accountId"))
//    .where(col("issuance_date") > col("date"))
//    .show(30)

//  private val transIdDuplicatesFinder = new TransIdDuplicatesFinder(cleanedDF)
//
//  private val suspiciousTransIds: DataFrame =
//    broadcast(transIdDuplicatesFinder.findCorruptedRecords()
//    .select(col("transId").as("suspiciousTransId")))
  ////
//  private val finalDF: DataFrame = cleanedDF.join(suspiciousTransIds,
//    col("transId") === col("sususpiciousTransId"),
//    "left_anti")spiciousTransId"),
//    "left_anti")
//
//  finalDF
//    .select(col("amount"), col("balance"))
//    .summary("count", "min", "25%", "50%", "75%", "70%" , "max", "mean", "stddev" )
//    .show()

}
