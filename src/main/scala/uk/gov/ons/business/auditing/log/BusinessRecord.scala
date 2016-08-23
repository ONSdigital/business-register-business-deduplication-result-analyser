package uk.gov.ons.business.auditing.log

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonInclude.Include

@JsonInclude(Include.NON_NULL)
case class BusinessRecord(id: String, matchKey: Option[String], matchScore: Option[Double])
