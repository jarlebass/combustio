package no.uit.sfb.toolwrapper

import java.io.{PrintStream, File}
import no.uit.sfb.fasta.{FastaReader, FastaWriter, FastaRecord}
import no.uit.sfb.mga.{MgaParser, MgaAnnotation}
import no.uit.sfb.toolabstraction.{StdStreams, ToolContext, ToolAbstraction, ToolFactory}


case class MgaArgs(inputFilePath: String)
case class MgaInput(contigs: Seq[FastaRecord])
case class MgaOutput(mgaAnnotations: Seq[MgaAnnotation], nucleotideGenes: Seq[FastaRecord])

class Mga extends ToolFactory[MgaInput, MgaOutput] {

  override def apply(toolContext: ToolContext, in: MgaInput):
                    ToolAbstraction[MgaOutput] = new ToolAbstraction[MgaOutput] {

    envPath = toolContext.path

    inputPath = s"${envPath}mgaInput/"
    outputPath = s"${envPath}mgaOutput/"

    inputFilePath = s"${inputPath}mga.in_${toolContext.index}"
    outputFilePath = s"${outputPath}mga.out_${toolContext.index}"

    override def execute(): Int = {
      createToolDir(inputPath)
      createToolDir(outputPath)

      if(in.contigs.isEmpty) {
        return EXIT_SUCCESS
      }

      // Write input to file to accommodate the tool
      val mgaInputFile = new File(inputFilePath)

      val fastaWriter = new FastaWriter(new PrintStream(mgaInputFile))
      in.contigs.foreach(fastaWriter.write)

      if(mgaInputFile.length() == 0) {
        return EXIT_SUCCESS
      }

      val mgaArgs = MgaArgs(inputFilePath = inputFilePath)
      val mgaCmd = buildCommand(mgaArgs)
      val stdStreams = StdStreams(s"mga.out_${toolContext.index}", s"stderr_${toolContext.index}")
      val cmdIsSeq = true
      val redirect = true

      val exitCode = execCommand(mgaCmd, outputPath, cmdIsSeq, redirect, stdStreams)

      exitSuccess(exitCode, toolContext.index, toolContext.uId)

      exitCode
    }

    override def output: MgaOutput = {
      if(!new File(outputFilePath).exists) {
        return MgaOutput(Seq.empty[MgaAnnotation], Seq.empty[FastaRecord])
      }

      // Parse
      val mgaParser = new MgaParser
      val mgaAnnotations: Seq[MgaAnnotation] = mgaParser(bufferedReaderFromFile(outputFilePath))

      // Extract annotated sequences and return the annotations and annotated sequences
      val fastaReader: Iterator[FastaRecord] = new FastaReader(bufferedReaderFromFile(inputFilePath))

      val nucleotideGenes: Seq[FastaRecord] = mgaAnnotations.flatMap{ mgaAnnotation =>
        val record: FastaRecord = fastaReader.find(record => record.header == mgaAnnotation.name)
          .getOrElse(throw new Exception("Could not find the MGA annotation name in the FASTA input file\n" +
            s"predictedGene.name:\t${mgaAnnotation.name}"))

        mgaAnnotation.predictedGenes.map{ predictedGene =>
          val header = s"${mgaAnnotation.name}_${predictedGene.geneId}".replaceAll(" ", "_")
          val sequence = record.sequence.substring(predictedGene.startPos, predictedGene.endPos)

          val nucleotideSequence = if(predictedGene.strand == "+") sequence else sequence.reverse

          FastaRecord(header = header, sequence = nucleotideSequence)
        }
      }


      MgaOutput(mgaAnnotations = mgaAnnotations, nucleotideGenes = nucleotideGenes)
    }

    override def command: Seq[String] = Seq(toolContext.program)

    def buildCommand(args: MgaArgs): Seq[String] = {
      command :+ args.inputFilePath
    }

  }

}
