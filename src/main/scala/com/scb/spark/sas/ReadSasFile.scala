package com.scb.spark.sas

import org.apache.spark.sql.SparkSession

object ReadSasFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SAS File Reader")
      .master("local[*]")
      .getOrCreate()

    // Replace with actual path to your .sas7bdat file
    //val sasPath = "C:\\Users\\Krishna\\Downloads\\naws_all\\naws_all.sas7bdat"
    //val sasPath = "C:\\Users\\Krishna\\Downloads\\crtd1103pub\\crtd1103pub.sas7bdat"
    val sasPath = "file:///C:/Users/Krishna/Downloads/leaders.sas7bdat"


    val df = spark.read
      .format("com.scb.spark.sas.SasDataSource")  // This matches what you define in SasDataSource.scala `shortName`
      .option("path", sasPath)
      .load()

    df.printSchema()
    df.show(10, truncate = false)

    spark.stop()
  }
}
