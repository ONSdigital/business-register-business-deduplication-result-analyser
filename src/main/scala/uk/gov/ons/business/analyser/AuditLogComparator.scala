package uk.gov.ons.business.analyser

import org.apache.spark.sql.{Dataset, SQLContext}
import uk.gov.ons.business.analyser.output.AuditLogComparison
import uk.gov.ons.business.auditing.log.LogEntry

class AuditLogComparator(firstLogEntries: Dataset[LogEntry], secondLogEntries: Dataset[LogEntry])(implicit sqlContext: SQLContext) {

  def compare(): Dataset[AuditLogComparison] = {
    import sqlContext.implicits._

    val joinedLogEntries: Dataset[(LogEntry, LogEntry)] = firstLogEntries.as("lhs").joinWith(secondLogEntries.as("rhs"), $"lhs.line.id" === $"rhs.line.id")

    joinedLogEntries.filter(pair => pair._1.result isDifferentThan  pair._2.result).map { case (lhs: LogEntry, rhs: LogEntry) =>
      AuditLogComparison(lhs.line.data, s"${lhs.result.category} => ${rhs.result.category}", lhs.result.records, rhs.result.records)
    }
  }

}
