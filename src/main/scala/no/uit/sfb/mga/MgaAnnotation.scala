package no.uit.sfb.mga

/**
 * Author: Inge Alexander Raknes
 */

case class MgaAnnotation(name: String, gc: Float, rbs: Float, self: String, predictedGenes: List[MgaPredictedGene])

case class MgaPredictedGene(
  geneId: String,
  startPos: Int,
  endPos: Int,
  strand: String,
  frame: Int,
  completePartial: String,
  geneScore: Float,
  usedModel: String,
  rbs: Option[Rbs]
)

case class Rbs(start: Int, end: Int, score: Float)
