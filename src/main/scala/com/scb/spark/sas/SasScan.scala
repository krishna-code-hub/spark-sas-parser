package com.scb.spark.sas

import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow

class SasScan(val path: String, val schema: StructType)
  extends Scan with Batch with Serializable {

  override def readSchema(): StructType = schema

  override def toString: String = s"SasScan(path=$path)"

  override def toBatch: Batch = this // ✅ required

  // ✅ This method comes from Batch, not Scan
  override def planInputPartitions(): Array[InputPartition] = {
    Array(SasPartition(path, schema))
  }


  override def createReaderFactory(): PartitionReaderFactory = {
    new PartitionReaderFactory with Serializable {
      override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
        val sasPartition = partition.asInstanceOf[SasPartition]
        new SasPartitionReader(sasPartition.path, sasPartition.schema)
      }
    }
  }
}
