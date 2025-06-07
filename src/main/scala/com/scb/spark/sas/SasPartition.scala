package com.scb.spark.sas

import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.StructType

case class SasPartition(path: String, schema: StructType) extends InputPartition with Serializable
