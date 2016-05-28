package no.uit.sfb.mga

import java.io.BufferedReader

/**
 * Author: Inge Alexander Raknes
 * Edited: Jarl Fagerli
 */

class MgaParser() {
  import MgaParser.regex._

  def apply(in: BufferedReader): Seq[MgaAnnotation] = {
    var annotations: List[MgaAnnotation] = Nil

    var line = in.readLine()
    var parsingHeader = true

    var _name, _gc, _rbs, _self = None : Option[String]
    var _predictedGenes: List[MgaPredictedGene] = Nil

    def clear(): Unit = {
      _name = None
      _gc = None
      _rbs = None
      _self = None
      _predictedGenes = Nil
    }

    def parseHeader(): Unit = {
      line match {
        case nameHeader(name) => _name = Some(name)
        case gcRbsHeader(gc, rbs) => _gc = Some(gc); _rbs = Some(rbs)
        case selfHeader(self) => _self = Some(self)
        case _ => {
          throw new IllegalStateException("Unrecognized header\n" +
                                          s"Line: $line")
        }
      }
    }

    def saveAnnotation(): Unit = {
      val annotation = MgaAnnotation(
        name = _name.get,
        gc = _gc.get.toFloat,
        rbs = _rbs.get.toFloat,
        self = _self.get,
        predictedGenes = _predictedGenes.reverse
      )
      annotations = annotation :: annotations
    }

    while(line != null && line != "") {
      if(line.startsWith("# ")) {
        if (!parsingHeader) {
          saveAnnotation()
          parsingHeader = true
          clear()
          parseHeader()
        } else {
          parseHeader()
        }
      } else {
        parsingHeader = false
        _predictedGenes = parsePredictedGene(line) :: _predictedGenes
      }
      line = in.readLine()
    }
    saveAnnotation()
    annotations.reverse
  }

  def parsePredictedGene(line: String): MgaPredictedGene = {
    val splits = line.split('\t')
    val rbs: Option[Rbs] = {
      val (start, end, score) = (splits(8), splits(9), splits(10))
      if(List(start, end, score).contains("-")) None
      else Some(Rbs(start.toInt, end.toInt, score.toFloat))
    }
    MgaPredictedGene(
      geneId = splits(0),
      startPos = splits(1).toInt,
      endPos = splits(2).toInt,
      strand = splits(3),
      frame = splits(4).toInt,
      completePartial = splits(5),
      geneScore = splits(6).toFloat,
      usedModel = splits(7),
      rbs = rbs
    )
  }
}

object MgaParser {
  private[mga] object regex {
    //val nameHeader = """^# ([^\s]+)$""".r
    val nameHeader = """^# ([\w\s-]+)$""".r
    val gcRbsHeader = """^# gc = (-?[\d]*\.?[\d]*), rbs = (-?[\d]*\.?[\d]*)$""".r
    val selfHeader = """^# self: (.+)$""".r
  }

  def translateId(parentId: String, geneId: String): String = {
    parentId + "?mga?" + geneId
  }
}
