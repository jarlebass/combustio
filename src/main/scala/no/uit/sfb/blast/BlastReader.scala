package no.uit.sfb.blast

import java.io.BufferedReader

/**
 * Author: Inge Alexander Raknes
 * Edited: Jarl Fagerli
 */

case class BlastRecord(queryId: String, subjectId: String, percentIdentity: Double,
                       alignmentLength: Int, mismatchCount: Int, gapOpenCount: Int,
                       queryStart: Int, queryEnd: Int, subjectStart: Int, subjectEnd: Int,
                       eValue: Double, bitScore: Double)

class BlastReader(in: BufferedReader) extends Iterator[BlastRecord] {
  private var nextLine = in.readLine()

  override def hasNext: Boolean = nextLine != null

  override def next(): BlastRecord = {
    if (!hasNext) {
      throw new NoSuchElementException("next on empty iterator")
    }

    val builder = new StringBuilder
    val splits = nextLine.split('\t')
    builder.append(splits(0))
    val queryId = builder.mkString

    builder.clear()
    builder.append(splits(1))

    val subjectId = builder.mkString

    nextLine = in.readLine()

    BlastRecord(
      queryId = queryId,
      subjectId = subjectId,
      percentIdentity = splits(2).toDouble,
      alignmentLength = splits(3).toInt,
      mismatchCount = splits(4).toInt,
      gapOpenCount = splits(5).toInt,
      queryStart = splits(6).toInt,
      queryEnd = splits(7).toInt,
      subjectStart = splits(8).toInt,
      subjectEnd = splits(9).toInt,
      eValue = splits(10).toDouble,
      bitScore = splits(11).toDouble
    )
  }
}
