package no.uit.sfb.testdata

import java.io.InputStream
import java.nio.file.Paths

object TestData {
  def streamFor(name: String): InputStream = getClass.getResourceAsStream(name)
  def pathFor(name: String): String = {
    val url = getClass.getResource(name)
    Paths.get(url.toURI).toAbsolutePath.toString
  }

  def hostfilePath = pathFor("hostfile")

  def testInputPath = "./scratch_space/"
  def testOutputPath = "./scratch_space/"

  def inputFastaStream = streamFor("input.fas")
  def inputFastaPath = pathFor("input.fas")

  def inputFastqStream = streamFor("input.fastq")
  def inputFastqPath = pathFor("input.fastq")

  def mgaOutStream = streamFor("mga.out")
  def mgaOutPath = pathFor("mga.out")

  def interProOutStream = streamFor("interpro.out")

  def predictedGenesProteinStream = streamFor("predicted_genes_protein.fas")
}
