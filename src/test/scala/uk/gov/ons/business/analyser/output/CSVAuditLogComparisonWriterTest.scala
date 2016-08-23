package uk.gov.ons.business.analyser.output

import java.io.File
import java.nio.file.Files

import org.apache.commons.io.FileUtils._
import org.apache.spark.sql.SQLContext
import org.scalatest.{FlatSpec, Matchers}
import uk.gov.ons.business.test.SparkContextsSuiteMixin
import uk.gov.ons.business.auditing.log.BusinessRecord

class CSVAuditLogComparisonWriterTest extends FlatSpec with SparkContextsSuiteMixin with Matchers {

  private val EMPTY = ""
  private val NEW_LINE_CHARACTER = "\n"

  behavior of "CSV writer for audit log comparison"

  it should "create empty file when comparison result collection is empty" in {
    val _sqlContext: SQLContext = sqlContext
    import _sqlContext.implicits._

    val outputPath = createTemporaryOutputPath("output.csv")
    new CSVAuditLogComparisonWriter().write(sqlContext.createDataset(Nil), outputPath)
    readFileToString(new File(s"$outputPath/part-00000")) should be(EMPTY)
  }

  it should "save all fields of comparison results into CSV file" in {
    val _sqlContext: SQLContext = sqlContext
    import _sqlContext.implicits._

    val outputPath = createTemporaryOutputPath("output.csv")
    new CSVAuditLogComparisonWriter().write(sqlContext.createDataset(AuditLogComparison("Kainos PLC,BT7 1NT,,763", "LineRejected => RecordCreated", Nil, BusinessRecord("RBIP#1", Some("KAINOSPLC"), None) :: Nil) :: Nil), outputPath)
    readFileToString(new File(s"$outputPath/part-00000")) should be("""Kainos PLC,BT7 1NT,,763,LineRejected => RecordCreated,[],"[{""id"":""RBIP#1"",""matchKey"":""KAINOSPLC""}]"""" + NEW_LINE_CHARACTER)
  }

  private def createTemporaryOutputPath(fileName: String): String = {
    s"${Files.createTempDirectory(getClass.getSimpleName)}/$fileName"
  }

}
