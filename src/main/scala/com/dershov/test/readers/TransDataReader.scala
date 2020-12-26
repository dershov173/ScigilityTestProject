package com.dershov.test.readers

import com.dershov.test.DataFrameReader
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

case object TransDataReader extends DataFrameReader {

  private def composeSchema(): StructType = {
    val fields = List(StructField("transId", DataTypes.IntegerType, nullable = false),
      StructField("accountId", DataTypes.IntegerType, nullable = false),
      StructField("date", DataTypes.DateType),
      StructField("type", DataTypes.StringType),
      StructField("operation", DataTypes.StringType),
      StructField("amount", DataTypes.IntegerType),
      StructField("balance", DataTypes.IntegerType),
      StructField("k_symbol", DataTypes.StringType),
      StructField("branch", DataTypes.StringType),
      StructField("bank", DataTypes.StringType),
      StructField("account", DataTypes.IntegerType))
    StructType(fields)
  }

  override val schema: StructType = composeSchema()
}
