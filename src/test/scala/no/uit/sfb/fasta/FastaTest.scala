package no.uit.sfb.fasta

import java.io.{PrintStream, InputStreamReader, BufferedReader}

import org.scalatest.{Matchers, FunSpec}
import no.uit.sfb.testdata.TestData

import scala.io.Source

/**
 * Author: Inge Alexander Raknes
 * Edited: Jarl Fagerli
 */

class FastaTest extends FunSpec with Matchers {
  describe("FastaReader") {
    it("should parse fasta correctly") {
      val fastaReader = new FastaReader(new BufferedReader(new InputStreamReader(TestData.predictedGenesProteinStream)))
      val fastaList = fastaReader.toList
      fastaList.take(2) shouldBe List(
        FastaRecord("Cellulose_genome_denovo_accurate_c1?mga?gene_1",
        "GLMVGLGETNEEILQVMRDLRSHGVTMLTIGQYLQPSKDHLPVERYVHPDEFNMFQEEAVKMGFEHAACGPLVRSSYHADKQAAGEEVK"),
        FastaRecord("Cellulose_genome_denovo_accurate_c1?mga?gene_2",
          "MNLKSLKINKGLGMKFFLSAPLLFTWLLAFSALATDKHQKPVVIFETTMGNIVIEVNNKQAPKSAKYFLSLIEQGKFNGTSFYRSGSV" +
          "AGKTPQFIEGGLVDKFILKGDITSVKNSGLPILDDFETTSYSKLKHQVATVSMARDILETGHAIPDIFICLDDIPSFDQNGRQKPDSR" +
          "GFPAFAKVIKGMDVVQKISNKERKGETHIKFLQGQILTTPVIILRAYRINIAQ"))
      fastaList.length shouldBe 5
    }
  }

  describe("FastaWriter") {
    it("should write write fasta correctly given FastaRecords"){
      val path = System.getProperty("java.io.tmpdir") + "/"
      val fPath = path + "test.fasta"
      val f = new java.io.File(fPath)
      val fastaIn = new FastaReader(new BufferedReader(new InputStreamReader(TestData.inputFastaStream)))
      val fastaWriter = new FastaWriter(new PrintStream(f), 60)

      fastaIn.foreach(fastaWriter.write)

      val in = Source.fromFile(TestData.inputFastaPath).getLines()
      val out = Source.fromFile(f).getLines()

      val diff = (in zip out).count { case (x, y) => x != y}

      f.delete()

      diff shouldBe 0
    }
  }
}
