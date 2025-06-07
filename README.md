# Spark SAS DataSource V2

This project provides a **Spark DataSource V2 connector** to read `.sas7bdat` SAS data files directly into Apache Spark DataFrames, with support for both **local filesystem**, **HDFS**, and **object storage** (e.g., S3).

> ✅ Compatible with Apache Spark 3.x  
> ✅ Built using the EPAM Parso library for parsing SAS files  
> ✅ Supports parallelized execution by implementing `DataSourceV2` with `Batch` mode  
> ✅ Output easily convertible to Parquet or any other format via Spark

---

## Features

- ✅ Plug-and-play `format("sas")` syntax for reading `.sas7bdat` files
- ✅ Support for any Hadoop-compatible filesystem: HDFS, S3, ABFS, etc.
- ✅ Modular implementation using Spark's DataSource V2 API
- ✅ Supports `inferSchema` and `read` methods using Parso
- ✅ Can be packaged as a JAR and reused across projects

---

## Usage

### 1. Include the JAR

Build the JAR using:

```bash
sbt clean package
```

sample code:
```java
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Read SAS File")
  .master("local[*]")
  .getOrCreate()

val df = spark.read
  .format("com.scb.spark.sas.SasDataSource") // Registered short name
  .load("hdfs:///path/to/your/file.sas7bdat")

df.printSchema()
df.show()

