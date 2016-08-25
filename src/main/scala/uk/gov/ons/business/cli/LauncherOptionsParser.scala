package uk.gov.ons.business.cli

class LauncherOptionsParser extends scopt.OptionParser[LauncherOptions]("business-deduplication-result-analyser") {
  head("ONS Business de-duplication result analyser")

  help("help")
  version("version")

  opt[String]("previousAuditLogPath").valueName("<path>").text("path to previous audit log (required)").required()
    .action(
      (file, arguments) => arguments.copy(previousAuditLogPath = file)
    )

  opt[String]("newAuditLogPath").valueName("<path>").text("path to new audit log (required)").required()
    .action(
      (file, arguments) => arguments.copy(newAuditLogPath = file)
    )

  opt[String]("outputFile").valueName("<path>").text("path to output file (required)").required()
    .action(
      (file, arguments) => arguments.copy(outputFilePath = file)
    )
}

