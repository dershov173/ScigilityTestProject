package com.dershov.test.readers

import com.dershov.test.DataFrameReader
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object AccountDataReader extends DataFrameReader {
  private def composeSchema() : StructType = {
    val fields = List(StructField("accountId", DataTypes.IntegerType, nullable = false),
      StructField("districtId", DataTypes.IntegerType),
      StructField("frequency", DataTypes.StringType),
      StructField("date", DataTypes.DateType))
    StructType(fields)
  }

  override val schema: StructType = composeSchema()
}
