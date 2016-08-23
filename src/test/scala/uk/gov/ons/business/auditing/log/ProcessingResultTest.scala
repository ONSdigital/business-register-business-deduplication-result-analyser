package uk.gov.ons.business.auditing.log

import org.scalatest.{FlatSpec, Matchers}

class ProcessingResultTest extends FlatSpec with Matchers {

  // business indexes
  private val newRecord = BusinessRecord("RBIP#1", Some("ONS Newport"), None)
  private val existingRecord = BusinessRecord("RBIP#1", None, Some(0.90))
  // processing results
  private val lineRejected = ProcessingResult("LineRejected", Nil)
  private val recordCreated = ProcessingResult("RecordCreated", newRecord :: Nil)
  private val recordMultiMatched = ProcessingResult("RecordMultiMatched", existingRecord :: Nil)

  behavior of "Processing result"

  it should "be classified as different when categories don't match" in {
    (lineRejected isDifferentThan recordCreated) should be(true)
  }

  it should "be classified as different when number of records don't match" in {
    (recordMultiMatched isDifferentThan recordMultiMatched.copy(records = existingRecord :: existingRecord :: Nil)) should be(true)
  }

  it should "be classified as different when match keys don't match (e.g. new processing result has uppercase match key)" in {
    (recordCreated isDifferentThan recordCreated.copy(records = newRecord.copy(matchKey = Some("ONS NEWPORT")) :: Nil)) should be(true)
  }

  it should "be classified as different when match scores don't match (e.g. new processing result has higher match score)" in {
    (recordMultiMatched isDifferentThan recordMultiMatched.copy(records = existingRecord.copy(matchScore = Some(0.95)) :: Nil)) should be(true)
  }

  it should "be classified as same in other cases" in {
    (lineRejected isDifferentThan lineRejected) should be(false)
    (recordCreated isDifferentThan recordCreated) should be(false)
    (recordMultiMatched isDifferentThan recordMultiMatched) should be(false)
  }

}
