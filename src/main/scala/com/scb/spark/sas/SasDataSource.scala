package com.scb.spark.sas

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.{StructType, DataType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import java.util
import scala.collection.JavaConverters._

class SasDataSource extends TableProvider {

  // Enable schema inference
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val path = options.get("path")
    require(path != null, "'path' must be specified to infer schema from SAS file")
    SasUtil.getSchema(path)
  }

  // Build table using inferred or user-provided schema
  override def getTable(structType: StructType, transforms: Array[Transform], options: util.Map[String, String]): Table = {
    val caseInsensitiveOptions = new CaseInsensitiveStringMap(options)
    val path = caseInsensitiveOptions.get("path")
    require(path != null, "'path' option is required for reading SAS files")

    new SasTable(path, structType) // Delegates to SasTable.scala
  }

  // For Spark < 3.2 compatibility
  override def supportsExternalMetadata(): Boolean = true
}
