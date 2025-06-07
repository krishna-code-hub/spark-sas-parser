package com.scb.spark.sas

import com.epam.parso.impl.SasFileReaderImpl
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._

object SasUtil {
  def getSchema(path: String): StructType = {
    val hadoopPath = new Path(path)
    val fs = FileSystem.get(hadoopPath.toUri, new Configuration())
    val inputStream = fs.open(hadoopPath)
    try {
      val reader = new SasFileReaderImpl(inputStream)

      val fields = reader.getColumns.asScala.toSeq.map { sasCol =>
        val colType = sasCol.getType match {
          case t if t == classOf[String]  => StringType
          case t if t == classOf[Double]  => DoubleType
          case t if t == classOf[Integer] => IntegerType
          case t if t == classOf[java.util.Date] => TimestampType
          case _                          => StringType
        }
        StructField(sasCol.getName, colType, nullable = true)
      }
      StructType(fields)
    } finally {
      inputStream.close()
    }
  }
}
