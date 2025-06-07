package com.scb.spark.sas

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import java.util

class SasTable(path: String, schema: StructType) extends Table with SupportsRead {
  override def name(): String = s"SasTable[$path]"
  override def schema(): StructType = schema
  override def capabilities(): util.Set[TableCapability] =
    util.EnumSet.of(TableCapability.BATCH_READ)

  override def newScanBuilder(options: org.apache.spark.sql.util.CaseInsensitiveStringMap): ScanBuilder =
    () => new SasScan(path, schema)
}

object SasTable {
  def inferSchema(path: String): StructType = SasUtil.getSchema(path)
}