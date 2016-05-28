package no.uit.sfb.toolwrapper

import java.io.{FileInputStream, PrintStream, File}
import java.net.InetAddress
import no.uit.sfb.toolabstraction.CommandHelper._
import no.uit.sfb.fasta.{FastaWriter, FastaRecord}
import no.uit.sfb.interproscan5.{InterProScanParser, InterProRecord}
import no.uit.sfb.toolabstraction.{StdStreams, ToolAbstraction, ToolContext, ToolFactory}

case class InterProScan5Args(format: String, applications: String, seqType: String,
                             inputFilePath: String, outputFilePath: String)
case class InterProScan5Input(predictedSequences: Seq[FastaRecord])
case class InterProScan5Output(interProRecords: Seq[InterProRecord])
class InterProScan5 extends ToolFactory[InterProScan5Input, InterProScan5Output] {

  override def apply(toolContext: ToolContext, in: InterProScan5Input):
                    ToolAbstraction[InterProScan5Output] = new ToolAbstraction[InterProScan5Output] {

    val hostName = InetAddress.getLocalHost.getHostName

    final val APPLICATIONS = "TIGRFAM,PRODOM,SMART,PROSITEPROFILES,HAMAP,SUPERFAMILY," +
                             "PRINTS,PANTHER,GENE3D,PIRSF,COILS"

    envPath = toolContext.path

    inputPath = s"${toolContext.path}interProScan5Input/"
    outputPath = s"${toolContext.path}interProScan5Output/"

    inputFilePath = s"${inputPath}interproscan5.in_${toolContext.index}"
    outputFilePath = s"${outputPath}interproscan5.out_${toolContext.index}"

    override def execute(): Int = {
      createToolDir(inputPath)
      createToolDir(outputPath)

      if(in.predictedSequences.isEmpty) {
        return EXIT_SUCCESS
      }

      val interProScan5InputFile = new File(inputFilePath)

      val fastaWriter = new FastaWriter(new PrintStream(interProScan5InputFile))
      in.predictedSequences.foreach(fastaWriter.write)

      if(interProScan5InputFile.length() == 0) {
        return EXIT_SUCCESS
      }

      val interProScan5Args = InterProScan5Args(
        format         = "tsv",
        applications   = APPLICATIONS,
        seqType        = "n",
        inputFilePath  = inputFilePath,
        outputFilePath = outputFilePath
      )
      val interProScan5Cmd = buildCommand(interProScan5Args)
      val stdStreams = StdStreams(s"stdout_${toolContext.index}", s"stderr_${toolContext.index}")
      val cmdIsSeq = true
      val redirect = true

      val exitCode = execCommand(interProScan5Cmd, envPath, cmdIsSeq, redirect, stdStreams)

      exitCode
    }

    override def output: InterProScan5Output = {
      var interProRecords: Seq[InterProRecord] = Seq.empty[InterProRecord]

      if(!new File(outputFilePath).exists) {
        return InterProScan5Output(Seq.empty[InterProRecord])
      }

      val interProScan5Parser = new InterProScanParser
      val interProReader: Iterator[InterProRecord] = interProScan5Parser(new FileInputStream(outputFilePath))
      while (interProReader.hasNext) {
        interProRecords :+= interProReader.next
      }


      InterProScan5Output(interProRecords = interProRecords)
    }

    override def command: Seq[String] = {
      val program = if(hostName.contains("compute-0")) INTERPROSCAN5_COMPUTE0 else INTERPROSCAN5_COMPUTE1
      Seq("bash", program)
    }

    def buildCommand(args: InterProScan5Args): Seq[String] = {
      command :+
        "-goterms"       :+
        "-iprlookup"     :+
        "-f"             :+ args.format :+
        "--applications" :+ args.applications :+
        "-t"             :+ args.seqType :+
        "-i"             :+ args.inputFilePath :+
        "-o"             :+ args.outputFilePath
    }

  }
}
