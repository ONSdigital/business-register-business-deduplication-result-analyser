package uk.gov.ons.business.auditing.log

import scala.collection.Seq

case class LogEntry(line: SourceLine, result: ProcessingResult)

case class SourceLine(id: String, data: String)

case class ProcessingResult(category: String, records: Seq[BusinessRecord]) {

  def isDifferentThan(other: ProcessingResult): Boolean = {
    this.category != other.category || this.records.size != other.records.size || !this.records.corresponds(other.records) { case (lhs, rhs) =>
      lhs.matchKey == rhs.matchKey && lhs.matchScore == rhs.matchScore
    }
  }

}
