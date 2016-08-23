package uk.gov.ons.business.cli

import java.nio.file.Files

import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class CommandLineLauncherIT extends FlatSpec with Matchers {

  behavior of "Audit log comparator (integration test)"

  it should "create audit log comparison result" in {
    val outputPath: String = createTemporaryOutputPath("comparison.csv")

    CommandLineLauncher.main(Array(
      "--previousAuditLogPath", getClass.getResource("/fixtures/it/audit-log-1.json").getPath,
      "--newAuditLogPath", getClass.getResource("/fixtures/it/audit-log-2.json").getPath,
      "--outputFile", outputPath
    ))

    val expectedLines: List[String] = Source.fromInputStream(getClass.getResourceAsStream("/fixtures/it/comparison.csv")).getLines.toList
    val actualLines: List[String] = Source.fromFile(s"$outputPath/part-00000").getLines.toList
    actualLines should contain theSameElementsAs expectedLines
  }

  private def createTemporaryOutputPath(fileName: String): String = {
    s"${Files.createTempDirectory(getClass.getSimpleName)}/$fileName"
  }

}
