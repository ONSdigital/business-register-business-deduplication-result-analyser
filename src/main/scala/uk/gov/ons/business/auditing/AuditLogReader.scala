package uk.gov.ons.business.auditing

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, SQLContext}
import uk.gov.ons.business.auditing.log.LogEntry

trait AuditLogReader {
  def read: Dataset[LogEntry]
}

class FileAuditLogReader(logFilePath: String)(implicit sqlContext: SQLContext) extends AuditLogReader {

  override def read: Dataset[LogEntry] = {
    import sqlContext.implicits._

    sqlContext.read.schema(schema).json(logFilePath).as[LogEntry]
  }

  private def schema = {
    StructType(StructField("line", lineStructType, nullable = false) :: StructField("result", resultStructType, nullable = false) :: Nil)
  }

  private def lineStructType: StructType = {
    StructType(StructField("id", StringType, nullable = false) :: StructField("data", StringType, nullable = false) :: Nil)
  }

  private def resultStructType: StructType = {
    StructType(StructField("category", StringType, nullable = false) :: StructField("records", recordsArrayType, nullable = true) :: Nil)
  }

  private def recordsArrayType: ArrayType = {
    ArrayType(StructType(StructField("id", StringType, nullable = false) :: StructField("matchKey", StringType, nullable = true) :: StructField("matchScore", DoubleType, nullable = true) :: Nil), containsNull = false)
  }
}
