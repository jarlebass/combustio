package no.uit.sfb.interproscan5

import no.uit.sfb.testdata.TestData
import org.scalatest.{Matchers, FunSpec}

/**
 * Author: Inge Alexander Raknes
 */

class InterProScanParserTest extends FunSpec with Matchers {
  def parseData() = {
    val parser = new InterProScanParser
    parser(TestData.interProOutStream).toSeq
  }

  describe("InterProScan parser") {
    it("should parse without exception") {
      val data = parseData()
    }

    it("should parse the tools correctly") {
      val data = parseData()
      val tools = data.map(_.analysis.toLowerCase).toSet
      val expectedTools = Set(
        "coils",
        "tigrfam",
        "prositeprofiles",
        "superfamily",
        "phobius",
        "smart",
        "gene3d",
        "prositepatterns",
        "pirsf",
        "panther",
        "prints",
        "hamap")
      expectedTools.subsetOf(tools).shouldBe(true)
    }
  }
}