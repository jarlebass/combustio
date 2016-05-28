package no.uit.sfb.fastq

import java.io.{PrintStream, BufferedReader}

import scala.collection.mutable.ArrayBuffer

case class FastqRecord(header: String, sequence: String, optHeader: String, qualityScores: String)

class FastqReader(in: BufferedReader) extends Iterator[FastqRecord] {
  private var nextHeader = in.readLine()

  override def hasNext: Boolean = nextHeader != null

  override def next(): FastqRecord = {
    val fastqFields = new ArrayBuffer[String](3)
    val header = nextHeader
    var line = ""
    while(line != null && !line.startsWith("@")) {
      fastqFields.append(line)
      line = in.readLine()
    }
    nextHeader = line
    FastqRecord(header.drop(1), fastqFields(1).toString, fastqFields(2).toString, fastqFields(3).toString)
  }
}

class FastqWriter(out: PrintStream) {
  def write(record: FastqRecord): Unit = {
    out.print("@")
    out.print(s"${record.header}\n" +
      s"${record.sequence}\n" +
      s"${record.optHeader}\n" +
      s"${record.qualityScores}\n")
  }
}
