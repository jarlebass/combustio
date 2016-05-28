package no.uit.sfb.interproscan5

import java.io.InputStream
import scala.io.Source
import scala.util.Try

/**
 * Author: Inge Alexander Raknes
 */

case class InterProRecord(
  proteinAccession: String,
  sequenceDigest: String,
  sequenceLength: Long,
  analysis: String,
  signatureAccession: String,
  signatureDescription: String,
  startLocation: Long,
  stopLocation: Long,
  score: Option[Double],
  status: Boolean,
  date: String,
  interProAccession: Option[String] = None,
  interProDescription: Option[String] = None,
  goAnnotations: List[String] = Nil,
  pathwayAnnotations: Option[String] = None
)

class InterProScanParser {
  def apply(in: InputStream): Iterator[InterProRecord] = {
    Source.fromInputStream(in).getLines().map { line =>
      val s = line.split('\t')
      val getOpt = s.lift
      InterProRecord(
        proteinAccession      = s(0),
        sequenceDigest        = s(1),
        sequenceLength        = s(2).toLong,
        analysis              = s(3),
        signatureAccession    = s(4),
        signatureDescription  = s(5),
        startLocation         = s(6).toLong,
        stopLocation          = s(7).toLong,
        score                 = Try(s(8).toDouble).toOption,
        status                = s(9) match {case "T" => true case "F" => false},
        date                  = s(10),
        interProAccession     = getOpt(11),
        interProDescription   = getOpt(12),
        goAnnotations         = getOpt(13).toList.flatMap(_.split('|')),
        pathwayAnnotations    = getOpt(14)
      )
    }
  }
}