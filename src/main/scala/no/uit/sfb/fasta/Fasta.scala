package no.uit.sfb.fasta

import java.io.{PrintStream, BufferedReader}

/**
 * Author: Inge Alexander Raknes
 */

case class FastaRecord(header: String, sequence: String)

class FastaReader(in: BufferedReader) extends Iterator[FastaRecord] {
  private var nextHeader = in.readLine()

  override def hasNext: Boolean = nextHeader != null

  override def next(): FastaRecord = {
    val sequenceBuilder = new StringBuilder
    val header = nextHeader
    var line = ""
    while(line != null && !line.startsWith(">")) {
      sequenceBuilder.append(line)
      line = in.readLine()
    }
    nextHeader = line
    FastaRecord(header.drop(1), sequenceBuilder.mkString)
  }
}

class FastaWriter(out: PrintStream, width: Int = 75) {
  def write(record: FastaRecord): Unit = {
    out.print(">")
    out.println(record.header)
    record.sequence.grouped(width).foreach(out.println)
  }
}
