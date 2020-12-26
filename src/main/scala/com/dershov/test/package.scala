package com.dershov

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

package object test {
  case class Transaction(transId:Int,
                         accountId:Int,
                         date:java.sql.Date,
                         _type:String,
                         operation:String,
                         amount:Int,
                         balance:Int,
                         k_symbol:String,
                         bank:String,
                         account:Int)

  trait DataFrameReader {

    val schema:StructType
    def readDataFrameFromCSV(pathToFile:String)(implicit spark: SparkSession): DataFrame = {
      spark.read
        .option("delimiter", ";")
        .option("header", true)
        .option("inferSchema", false)
        .schema(schema)
        .csv(pathToFile)
    }
  }

  trait DataQualityCheck {
    def findCorruptedRecords():DataFrame
  }

  trait Repair {
    def repairData(): DataFrame
  }

  trait Pipeline {
    def performExtractCheckRepairPipeline() : DataFrame
  }
}
