package no.uit.sfb.toolwrapper

import java.io.{PrintStream, File}
import java.net.InetAddress
import no.uit.sfb.blast.{BlastReader, BlastRecord}
import no.uit.sfb.fasta.{FastaWriter, FastaRecord}
import no.uit.sfb.toolabstraction.{StdStreams, ToolAbstraction, ToolContext, ToolFactory}

case class BlastArgs(databasePath: String, query: String, outputFilePath: String,
                     maxTargetSeqs: String, format: String, eValue: String, dbSize: String)
case class BlastInput(predictedSequences: Seq[FastaRecord], blastDbSize: Long)
case class BlastOutput(blastRecords: Seq[BlastRecord])
class Blast extends ToolFactory[BlastInput, BlastOutput] {

  override def apply(toolContext: ToolContext, in: BlastInput):
                    ToolAbstraction[BlastOutput] = new ToolAbstraction[BlastOutput] {

    val hostName = InetAddress.getLocalHost.getHostName

    def BLAST_DB_PATH(splitNum: Int) = s"/home/jfa012/toolabstraction/bio_tools/BLAST/uniref50_8_split/split_$splitNum/uniref50_split_$splitNum"
    final val BLAST_DB_SPLITS = 8
    final val BLAST_DB_SOURCE_SIZE = in.blastDbSize.toString
    final val BLAST_NUM_THREADS = if(hostName.contains("compute-0")) 2 else 4

    envPath = toolContext.path

    inputPath = s"${toolContext.path}blastInput/"
    outputPath = s"${toolContext.path}blastOutput/"

    inputFilePath = s"${inputPath}blast.in_${toolContext.index}"
    outputFilePath = s"${outputPath}blast.out_${toolContext.index}"

    override def execute(): Int = {
      createToolDir(inputPath)
      createToolDir(outputPath)

      val blastInputFile = new File(inputFilePath)
      val fastaWriter = new FastaWriter(new PrintStream(blastInputFile))
      in.predictedSequences.foreach(fastaWriter.write)

      if (blastInputFile.length() == 0) {
        return EXIT_SUCCESS
      }

      for (split <- 1 to BLAST_DB_SPLITS) {
        val blastArgs    = BlastArgs(
          databasePath   = BLAST_DB_PATH(split),
          query          = inputFilePath,
          outputFilePath = s"${outputFilePath}_$split",
          maxTargetSeqs  = "5",
          format         = "6",
          eValue         = "1e-5",
          dbSize         = BLAST_DB_SOURCE_SIZE
        )

        val blastCmd = buildCommand(blastArgs)
        println(s"\n\n${dateTime()} INFO BlastWrapper: ${blastCmd.mkString(" ")}\n\n")
        val stdStreams = StdStreams(s"stdout_${toolContext.index}_$split", s"stderr_${toolContext.index}_$split")
        val cmdIsSeq = true
        val redirect = true

        execCommand(blastCmd, outputPath, cmdIsSeq, redirect, stdStreams)
      }

      exitSuccess(EXIT_SUCCESS, toolContext.index, toolContext.uId)

      EXIT_SUCCESS
    }

    override def output: BlastOutput = {
      var blastRecords: Seq[BlastRecord] = Seq.empty[BlastRecord]

      for (split <- 1 to BLAST_DB_SPLITS) {
        if(new File(s"${outputFilePath}_$split").exists) {
          val blastReader: BlastReader = new BlastReader(bufferedReaderFromFile(s"${outputFilePath}_$split"))
          while (blastReader.hasNext) {
            blastRecords :+= blastReader.next
          }
        }
      }

      BlastOutput(blastRecords = blastRecords)
    }

    override def command: Seq[String] = Seq(toolContext.program)

    def buildCommand(args: BlastArgs): Seq[String] = {
      command :+
        "-db"              :+ args.databasePath :+
        "-query"           :+ args.query:+
        "-out"             :+ args.outputFilePath :+
        "-max_target_seqs" :+ args.maxTargetSeqs :+
        "-outfmt"          :+ args.format :+
        "-evalue"          :+ args.eValue :+
        "-dbsize"          :+ args.dbSize :+
        "-num_threads"     :+ BLAST_NUM_THREADS.toString
    }

    // Need to customize the recovery function to reflect the output files produced due to
    // running BLAST a total 8 times because of the database splits
    override def recoverable(index: Int, uId: String): Boolean = {
      if(new File(successFilePath(index)).exists()) {
        val content = scala.io.Source.fromFile(successFilePath(index))
        try {
          val lines = fileToString(successFilePath(index)).split('\n')

          if(!(lines(0).toInt == EXIT_SUCCESS && lines(1) == uId)) {
            return false
          }
        } catch {
          case e: Exception => {
            println(e)
            return false
          }
        } finally content.close()

        for (split <- 1 to BLAST_DB_SPLITS) {
          if(!new File(s"${outputFilePath}_$split").exists) {
            return false
          }
        }

        println(s"\n\n${dateTime()} INFO ToolAbstraction: Local recoverable:\n")
        for (split <- 1 to BLAST_DB_SPLITS) {
          println(s"\tsuccessFilePath: ${successFilePath(index)}\toutputFilePath: ${outputFilePath}_$split\n\n")
        }
        true
      } else {
        println(s"\n\n${dateTime()} INFO ToolAbstraction: No local recoverable output identified, " +
          "must perform computation ...\n\n")
        false
      }
    }

  }
}
