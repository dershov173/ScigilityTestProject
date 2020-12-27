package com.dershov.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}

class ShaIntegrityTest extends FlatSpec with Matchers {
  implicit val session: SparkSession = SparkSession.builder()
    .master("local[4]")
    .appName("CSV_Reader_Test")
    .getOrCreate()
  session

  "sha1" should "return the same hash for the same strings" in {
    import org.apache.spark.sql.functions._
    val strings = Seq("Spark", "spark", "Spark", "Hadoop")

    val rdd : RDD[Row] = session.sparkContext.parallelize(strings).map(Row(_))
    session.createDataFrame(rdd, StructType(Array(StructField("name", DataTypes.StringType))))
      .withColumn("hash", sha1(col("name")))
      .distinct()
      .count() should equal(3)

  }

}
