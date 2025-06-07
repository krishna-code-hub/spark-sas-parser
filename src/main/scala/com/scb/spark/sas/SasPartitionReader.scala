package com.scb.spark.sas

import com.epam.parso.impl.SasFileReaderImpl
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.InputStream
import java.net.URI
import java.util.Date

class SasPartitionReader(path: String, schema: StructType)
  extends PartitionReader[InternalRow] with Serializable {

  private val inputStream: InputStream = {
    val conf = new Configuration()
    val fsPath = new Path(path)
    val fs = FileSystem.get(fsPath.toUri, conf)
    fs.open(fsPath )
  }

  private val reader = new SasFileReaderImpl(inputStream)
  private val rows = reader.readAll().iterator

  override def next(): Boolean = rows.hasNext

  override def get(): InternalRow = {
    val rawRow = rows.next().toArray
    val converted = new Array[Any](schema.length)

    for (i <- schema.indices) {
      val value = rawRow(i)
      converted(i) = if (value == null) {
        null
      } else {
        schema(i).dataType match {
          case StringType => UTF8String.fromString(value.toString)
          case IntegerType => value.asInstanceOf[Number].intValue()
          case LongType => value.asInstanceOf[Number].longValue()
          case DoubleType => value.asInstanceOf[Number].doubleValue()
          case FloatType => value.asInstanceOf[Number].floatValue()
          case BooleanType => value.asInstanceOf[Boolean]
          case DateType => value match {
            case d: Date => (d.getTime / (24L * 60 * 60 * 1000)).toInt
            case _ => null
          }
          case TimestampType => value match {
            case d: Date => d.getTime * 1000L // microseconds
            case _ => null
          }
          case _ => UTF8String.fromString(value.toString)
        }
      }
    }

    new GenericInternalRow(converted)
  }

  override def close(): Unit = inputStream.close()
}
