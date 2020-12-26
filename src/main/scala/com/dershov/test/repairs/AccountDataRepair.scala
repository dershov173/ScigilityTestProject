package com.dershov.test.repairs

import com.dershov.test.Repair
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

case class AccountDataRepair(rawTransDF:DataFrame,
                             suspiciousAccountIds:DataFrame) extends Repair {
  override def repairData(): DataFrame = {
    rawTransDF.join(suspiciousAccountIds,
      col("accountId") === col("suspiciousAccountId"),
      "left_anti")
  }
}
