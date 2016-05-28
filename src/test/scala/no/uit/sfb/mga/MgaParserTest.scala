package no.uit.sfb.mga

import java.io.{InputStreamReader, BufferedReader}

import org.scalatest.{Matchers, FunSpec}
import no.uit.sfb.testdata.TestData

/**
 * Author: Inge Alexander Raknes
 */

class MgaParserTest extends FunSpec with Matchers {
  import MgaParser.regex._
  describe("MgaParser regex") {
    it("should parse a name header correctly") {
      val line = "# Cellulose_genome_denovo_accurate_c8"
      val nameHeader(name) = line
      name shouldBe "Cellulose_genome_denovo_accurate_c8"
    }
    it("should parse a gc/rbs header correctly") {
      val line = "# gc = 0.313883, rbs = -1"
      val gcRbsHeader(gc, rbs) = line
      (gc,rbs) shouldBe ("0.313883", "-1")
    }
    it("should parse a self header correctly") {
      val line = "# self: -"
      val selfHeader(self) = line
      self shouldBe "-"
    }
  }

  describe("MgaParser") {

    def mgaReader() = new BufferedReader(new InputStreamReader(TestData.mgaOutStream))

    it("should correctly parse example data") {
      val mgaParser = new MgaParser
      val elements = mgaParser(mgaReader())
      elements shouldBe List(
        MgaAnnotation("Cellulose_genome_denovo_accurate_c1", 0.358747f, -1, "b", List(
          MgaPredictedGene("gene_1", 1, 270, "+", 0, "01", 37.4191f, "s", None),
          MgaPredictedGene("gene_2", 652, 1341, "-", 0, "11", 68.408f, "s", Some(Rbs(1349, 1354, -0.284637f))),
          MgaPredictedGene("gene_7", 8114, 8687, "+", 0, "10", 63.002f, "s", None))),
        MgaAnnotation("Cellulose_genome_denovo_accurate_c2", 0.365548f, -1, "-", List(
          MgaPredictedGene("gene_1", 1, 430, "+", 1, "01", 66.3785f, "b", None),
          MgaPredictedGene("gene_2", 477, 1586, "-", 0, "11", 77.5769f, "b", Some(Rbs(1593,1598,-0.210196f)))
        ))
      )
    }
  }
}
