package com.dershov.test.readers

import com.dershov.test.DataFrameReader
import org.apache.spark.sql.types._

object LoanDataReader extends DataFrameReader{
  def composeSchema():StructType = {
    val fields = List(StructField("loanId", IntegerType, false),
      StructField("accountId", IntegerType, false),
      StructField("date", DateType),
      StructField("amount", IntegerType),
      StructField("duration", IntegerType),
      StructField("payments", FloatType),
      StructField("status", StringType)
    )
    StructType(fields)
  }

  override val schema: StructType = composeSchema()
}
