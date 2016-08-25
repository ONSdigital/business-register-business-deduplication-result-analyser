package uk.gov.ons.business.auditing

import java.io.File

import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, Matchers}
import uk.gov.ons.business.test.SparkContextsSuiteMixin
import uk.gov.ons.business.auditing.log._

class FileAuditLogReaderTest extends FlatSpec with SparkContextsSuiteMixin with Matchers {

  behavior of "File audit log reader"

  it should "read data from sample input file in JSON format" in {
    val dataset: Dataset[LogEntry] = new FileAuditLogReader(fixturePath("sample-audit-log.json")).read

    def assertNumberOfEntries(filterFunction: (LogEntry) => Boolean, expectedNumberOfEntries: Int): Unit = {
      dataset.filter(filterFunction).count() should be(expectedNumberOfEntries)
    }

    assertNumberOfEntries(_ => true, 5)
    assertNumberOfEntries(_ == LogEntry(SourceLine("a665a451", "Kainos PLC,4-6 Upper Crescent,BT7 1NT,,763"), ProcessingResult("LineRejected", null)), 1)
    assertNumberOfEntries(_ == LogEntry(SourceLine("a665a452", "Mikrotechnic,13 Ivy Lane,CT3 4GY,,14"), ProcessingResult("RecordCreated", BusinessRecord("RBIP#101", Some("MIKROTECHNIC"), None) :: Nil)), 1)
    assertNumberOfEntries(_ == LogEntry(SourceLine("a665a453", "Sunny's Surplus,72 Boat Lane,RG9 6JP,,8"), ProcessingResult("RecordMatched", BusinessRecord("RBIP#100", Some("SUNNYSSURPLUS"), None) :: Nil)), 1)
    assertNumberOfEntries(_ == LogEntry(SourceLine("a665a454", "Indiewealth,85 Trinity Crescent,S30 2BG,,87"), ProcessingResult("RecordAlmostMatched", BusinessRecord("RBIP#10", None, Some(0.90)) :: Nil)), 1)
    assertNumberOfEntries(_ == LogEntry(SourceLine("a665a455", "Isaly's,72 Lammas Street,TS9 9NU,,2"), ProcessingResult("RecordAlmostMatched", BusinessRecord("RBIP#20", None, Some(0.98)) :: BusinessRecord("RBIP#30", None, Some(0.99)) :: Nil)), 1)
  }

  private def fixturePath(fileName: String): String = {
    getClass.getResource(s"/fixtures/$fileName").getFile
  }
}
