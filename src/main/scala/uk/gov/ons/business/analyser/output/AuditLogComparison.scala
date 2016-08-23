package uk.gov.ons.business.analyser.output

import uk.gov.ons.business.auditing.log.BusinessRecord

case class AuditLogComparison(lineData: String,
                              categoryChange: String,
                              recordsFromFirstAuditLog: Seq[BusinessRecord],
                              recordsFromSecondAuditLog: Seq[BusinessRecord])
