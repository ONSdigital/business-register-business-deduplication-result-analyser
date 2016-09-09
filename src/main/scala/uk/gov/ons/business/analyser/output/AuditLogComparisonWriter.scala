package uk.gov.ons.business.analyser.output

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.csv.CSVFormat.EXCEL
import org.apache.spark.sql.{Dataset, SQLContext}
import uk.gov.ons.business.analyser.output.AuditLogComparisonWriter.mapper
import uk.gov.ons.business.auditing.log.BusinessRecord

import scala.language.implicitConversions

object AuditLogComparisonWriter {
  lazy val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
}

trait FileAuditLogComparisonWriter extends Serializable {
  def write(data: Dataset[AuditLogComparison], path: String): Unit
}

class CSVAuditLogComparisonWriter(implicit sqlContext: SQLContext) extends FileAuditLogComparisonWriter {

  override def write(data: Dataset[AuditLogComparison], path: String): Unit = {
    import sqlContext.implicits._

    data.map(toCSV).rdd.saveAsTextFile(path)
  }

  private def toCSV(comparison: AuditLogComparison): String = {
    implicit def recordsToString(records: Seq[BusinessRecord]): String = mapper.writeValueAsString(records)

    val resultFields: List[String] = List(comparison.categoryChange, comparison.recordsFromFirstAuditLog, comparison.recordsFromSecondAuditLog)
    comparison.lineData + EXCEL.getDelimiter + EXCEL.format(resultFields: _*)
  }

}
