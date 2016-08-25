package uk.gov.ons.business.cli

import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.ons.business.analyser.AuditLogComparator
import uk.gov.ons.business.analyser.output.{AuditLogComparison, CSVAuditLogComparisonWriter}
import uk.gov.ons.business.auditing.FileAuditLogReader

class Launcher {

  def launch(args: Array[String]) {
    new LauncherOptionsParser().parse(args, LauncherOptions(null, null, null)) match {
      case Some(arguments) =>
        val sparkConfig = new SparkConf().setAppName("business-deduplication-result-analyser")
        if (!sparkConfig.contains("spark.master")) {
          sparkConfig.setMaster(sys.env.getOrElse("SPARK_MODE", "local[*]"))
        }

        val context = new SparkContext(sparkConfig)
        implicit val sqlContext: SQLContext = new SQLContext(context)

        val result: Dataset[AuditLogComparison] = new AuditLogComparator(
          new FileAuditLogReader(arguments.previousAuditLogPath).read,
          new FileAuditLogReader(arguments.newAuditLogPath).read
        ).compare()

        new CSVAuditLogComparisonWriter().write(result, arguments.outputFilePath)

        context.stop()
      case None => // parser will print error in that case
    }
  }

}

object Application extends App {

  new Launcher().launch(args)

}
