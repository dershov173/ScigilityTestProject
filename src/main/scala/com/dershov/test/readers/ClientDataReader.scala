package com.dershov.test.readers

import com.dershov.test.DataFrameReader
import org.apache.spark.sql.types._


class ClientDataReader extends DataFrameReader{
  def composeSchema():StructType = {
    val fields = List(StructField("clientId", IntegerType, false),
      StructField("gender", StringType),
      StructField("birthDate", DateType),
      StructField("districtId", IntegerType))
    StructType(fields)
  }

  override val schema: StructType = composeSchema()
}
