package uk.gov.ons.business.analyser

import org.apache.spark.sql.{Dataset, SQLContext}
import org.scalatest.{FlatSpec, Matchers}
import uk.gov.ons.business.test.SparkContextsSuiteMixin
import uk.gov.ons.business.analyser.output.AuditLogComparison
import uk.gov.ons.business.auditing.log.{BusinessRecord, LogEntry, ProcessingResult, SourceLine}

class AuditLogComparatorTest extends FlatSpec with SparkContextsSuiteMixin with Matchers {

  behavior of "Audit log comparator"

  it should "return empty data set when both data sets are the same" in {
    val _sqlContext: SQLContext = sqlContext
    import _sqlContext.implicits._

    val logEntry: LogEntry = LogEntry(SourceLine("#1", "Kainos PLC"), ProcessingResult("LineRejected", Nil))

    val result: Dataset[AuditLogComparison] = new AuditLogComparator(List(logEntry).toDS(), List(logEntry).toDS()).compare()

    result.count() should be (0)
  }

  it should "return audit log comparison result for entries which has changed" in {
    val _sqlContext: SQLContext = sqlContext
    import _sqlContext.implicits._

    val firstLogEntry = LogEntry(SourceLine("#1", "Kainos PLC"), ProcessingResult("LineRejected", Nil))
    val secondLogEntry = LogEntry(SourceLine("#1", "Kainos PLC"), ProcessingResult("RecordCreated", BusinessRecord("RBIP#1", Some("KAINOSPLC"), None) :: Nil))

    val result: Dataset[AuditLogComparison] = new AuditLogComparator(List(firstLogEntry).toDS(), List(secondLogEntry).toDS()).compare()

    result.count() should be (1)
    result.first() should be (AuditLogComparison("Kainos PLC", "LineRejected => RecordCreated", Nil, BusinessRecord("RBIP#1", Some("KAINOSPLC"), None) :: Nil))
  }

}
