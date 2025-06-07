package com.scb.spark.sas

object Sample extends App {
  val schema = SasUtil.getSchema("C:\\Users\\Krishna\\Downloads\\naws_all\\naws_all.sas7bdat")
  println(schema.prettyJson)
}
