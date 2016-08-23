package uk.gov.ons.business.analyser.output

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.csv.CSVFormat.EXCEL
import org.apache.spark.sql.{Dataset, SQLContext}
import uk.gov.ons.business.analyser.output.AuditLogComparisonWriter.mapper

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
    val lineDataFields: Array[String] = comparison.lineData.split(EXCEL.getDelimiter)
    val comparisonFields: Array[String] = Array(
      comparison.categoryChange,
      mapper.writeValueAsString(comparison.recordsFromFirstAuditLog),
      mapper.writeValueAsString(comparison.recordsFromSecondAuditLog)
    )

    EXCEL.format((lineDataFields :: comparisonFields :: Nil).flatten: _*)
  }

}
