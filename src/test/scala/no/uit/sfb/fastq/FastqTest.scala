package no.uit.sfb.fastq

import java.io.{PrintStream, InputStreamReader, BufferedReader}

import no.uit.sfb.testdata.TestData
import org.scalatest.{Matchers, FunSpec}

import scala.io.Source

class FastqTest extends FunSpec with Matchers {
  describe("FastqReader") {
    it("should parse fastq correctly") {
      val fastqReader = new FastqReader(new BufferedReader(new InputStreamReader(TestData.inputFastqStream)))

      val fastqList = fastqReader.toList
      fastqList.take(2) shouldBe List(
        FastqRecord("M01337:7:000000000-A329T:1:1101:13631:2127 1:N:0:11",
          "ATCCTACACAGAGAAGGGCGCATGGCCTCGCTGAAAGACNTTGCNNNNNNNNCNNNGNNNNNNNNGNNGACNNTNNCNNNNNNNNNCAACGAGNNNNNNNANNTNNNGCNAGACNNNNNNNNNNGCGTCAAGCTGGCGANTGACCAACTGGATNACGNGCNNGNNNNNNNTGCCCNGNNNNNNNGCGGCGANAGCAGCCGGGTGCANNNGCTCGNCNNNNNNGCGNNNNNNCNNGNNNCCACGCCGTTCT",
          "+",
          "?????BB?<<BBBDBBFECCDFBEFFHHHHHHEHFFHHH#7AED########5###5########5##555##5##5#########66?FF;D#######6##4###44#44;*##########0000AEEEEFEFE>;#008?EFE8:AE*0#0)0#)0##)#######0))88#0#######0008??'#00)88:A'82.40*###.0)00#)######0))######)##0###000A88C'..*?"),
        FastqRecord("M01337:7:000000000-A329T:1:1101:16413:2127 1:N:0:11",
          "CGCCTGGCCTGTTGGCGCGACACCCTGTATCAGGCGGGCNTTGCNNNNNNNNANNNNNNNNNNNNCNNCTNNNNNNCNNNNNNNNNCTGNGTANNNNNNNTNNNNNNATNCGGCNNNNNNNNNNATATCACGGCGATGCNNGTCGCCAGCGATNAGANNNNNTNNNNNNNCATCANCNNNNNNNGGCANNTNAACAAGCGCATACNNNNNNNNNNGNNNNNNATTNNNNNNNNNGNNNNCCCCGACAGCCA",
          "+",
          "???????BDBBDDBBDCEDBCFHHHHHHHHHHHHHHHHH#5+AE########+############4##44######4#########4*4#4*4#######0######00#.088##########00.8AE:AA;>88A?##0008AA8;E82?#.0)#####)#######00.88#)#######00.0##)#)0008A*888?AE##########0######)0)#########0####.00?84;>;*00")
      )
      fastqList.length shouldBe 25
    }
  }

  describe("FastqWriter") {
    it("should write write fastq correctly given FastqRecords"){
      val path = System.getProperty("java.io.tmpdir") + "/"
      val fPath = path + "test.fastq"
      val f = new java.io.File(fPath)
      val fastqIn = new FastqReader(new BufferedReader(new InputStreamReader(TestData.inputFastqStream)))
      val fastqWriter = new FastqWriter(new PrintStream(f))

      fastqIn.foreach(fastqWriter.write)

      val in = Source.fromFile(TestData.inputFastqPath).getLines()
      val out = Source.fromFile(f).getLines()

      val diff = (in zip out).count { case (x, y) => x != y}

      f.delete()

      diff shouldBe 0
    }
  }
}
