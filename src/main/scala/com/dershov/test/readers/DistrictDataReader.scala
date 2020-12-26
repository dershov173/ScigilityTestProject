package com.dershov.test.readers

import com.dershov.test.DataFrameReader
import org.apache.spark.sql.types._

object DistrictDataReader extends DataFrameReader {
  private def composeSchema(): StructType = {
    val fields = List(StructField("districtId", IntegerType, nullable = false),
      StructField("A2", StringType),
      StructField("A3", StringType),
      StructField("A4", IntegerType),
      StructField("A5", IntegerType),
      StructField("A6", IntegerType),
      StructField("A7", IntegerType),
      StructField("A8", IntegerType),
      StructField("A9", IntegerType),
      StructField("A10", FloatType),
      StructField("A11", FloatType),
      StructField("A12", FloatType),
      StructField("A13", FloatType),
      StructField("A14", IntegerType),
      StructField("A15", IntegerType),
      StructField("A16", IntegerType))
    StructType(fields)
  }

  override val schema:StructType = composeSchema()


}
