package uk.gov.ons.business.cli

import java.io.File

import org.apache.spark.sql.{Dataset, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import uk.gov.ons.business.analyser.AuditLogComparator
import uk.gov.ons.business.analyser.output.{AuditLogComparison, CSVAuditLogComparisonWriter}
import uk.gov.ons.business.auditing.FileAuditLogReader

object CommandLineLauncher extends App {

  val parser = new scopt.OptionParser[LauncherOptions]("business-deduplication-result-analyser") {
    head("ONS Business de-duplication result analyser")

    help("help")
    version("version")

    opt[File]("previousAuditLogPath").valueName("<path>").text("path to previous audit log (required)").required()
      .action(
        (file, arguments) => arguments.copy(previousAuditLogPath = file)
      )

    opt[File]("newAuditLogPath").valueName("<path>").text("path to new audit log (required)").required()
      .action(
        (file, arguments) => arguments.copy(newAuditLogPath = file)
      )

    opt[File]("outputFile").valueName("<path>").text("path to output file (required)").required()
      .action(
        (file, arguments) => arguments.copy(outputFile = file)
      )
  }

  parser.parse(args, LauncherOptions(null, null, null)) match {
    case Some(arguments) =>
      val sparkConfig = new SparkConf().setAppName("business-deduplication-result-analyser")
      if (!sparkConfig.contains("spark.master")) {
        sparkConfig.setMaster(sys.env.getOrElse("SPARK_MODE", "local[*]"))
      }

      implicit val sqlContext: SQLContext = new SQLContext(new SparkContext(sparkConfig))

      val result: Dataset[AuditLogComparison] = new AuditLogComparator(
        new FileAuditLogReader(arguments.previousAuditLogPath).read,
        new FileAuditLogReader(arguments.newAuditLogPath).read
      ).compare()

      new CSVAuditLogComparisonWriter().write(result, arguments.outputFile.getAbsolutePath)
    case None =>
      System.exit(1);
  }
}
