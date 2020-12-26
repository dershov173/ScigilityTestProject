package com.dershov.test

import com.dershov.test.checks.SuspiciousAccountIdsFinder
import com.dershov.test.readers.{AccountDataReader, TransDataReader}
import com.dershov.test.repairs.AccountDataRepair
import org.apache.spark.sql.{DataFrame, SparkSession}

class AccountIdsExtractCheckRepairPipeline(implicit val spark:SparkSession) extends Pipeline {
  override def performExtractCheckRepairPipeline(): DataFrame = {
    val pathToTransTable = this.getClass.getResource("trans.csv").toString
    val rawTransTable = TransDataReader.readDataFrameFromCSV(pathToTransTable)

    val pathToAccountTable = this.getClass.getResource("account.csv").toString
    val rawAccountTable = AccountDataReader.readDataFrameFromCSV(pathToAccountTable)

    val suspiciousAccountIdsFinder = SuspiciousAccountIdsFinder(rawTransTable, rawAccountTable)
    val suspiciousAccountIds = suspiciousAccountIdsFinder.findCorruptedRecords()

    val accountDataRepair = AccountDataRepair(rawTransTable, suspiciousAccountIds)
    accountDataRepair.repairData()
  }
}
