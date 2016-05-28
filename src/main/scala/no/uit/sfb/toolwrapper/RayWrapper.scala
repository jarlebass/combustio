package no.uit.sfb.toolwrapper

import java.io.File
import java.nio.file.{Paths, StandardCopyOption, Files}
import no.uit.sfb.fasta.{FastaReader, FastaRecord}
import no.uit.sfb.toolabstraction._
import org.apache.commons.io.FileUtils.moveDirectory

case class RayArgs(mca: String, procs: String, hostFilePath: String,
                   kmerLength: String, leftSequence: String, rightSequence: String,
                   minContigLength: String, outputDirectory: String)
case class RayInput(leftInputFilePath: String, rightInputFilePath: String)
case class RayOutput(contigs: Seq[FastaRecord])

class Ray extends ToolFactory[RayInput, RayOutput] {

  override def apply(toolContext: ToolContext, in: RayInput):
                    ToolAbstraction[RayOutput] = new ToolAbstraction[RayOutput] {

    final val HOSTFILE: String = "/home/jfa012/toolabstraction/bio_tools/RAY/hostfile"
    final val NETWORK_INTERFACE_CONTROLLER: String = "btl_tcp_if_exclude docker0,lo"

    envPath = toolContext.path

    inputPath = s"${toolContext.path}rayInput/"
    outputPath = s"${toolContext.path}rayOutput"
    outputFilePath = s"$outputPath/Contigs.fasta"

    val nfsTmp = s"/home/jfa012/toolabstraction/tmp/"
    val nfsInputPath = s"${nfsTmp}rayInput/"
    val nfsOutputPath = s"${nfsTmp}rayOutput"

    override def execute(): Int = {
      createToolDir(envPath)
      createToolDir(nfsInputPath)

      val (left, right) = {
        (convertInputPath(in.leftInputFilePath, nfsInputPath),
         convertInputPath(in.rightInputFilePath, nfsInputPath))
      }

      Files.move(
        Paths.get(in.leftInputFilePath),
        Paths.get(left),
        StandardCopyOption.REPLACE_EXISTING
      )
      Files.move(
        Paths.get(in.rightInputFilePath),
        Paths.get(right),
        StandardCopyOption.REPLACE_EXISTING
      )

      val hostFilePath = s"${envPath}hostfile"
      val mpiHosts = fileToString(HOSTFILE)
      stringToFile(mpiHosts, hostFilePath)

      // Ignore docker0 and lo interfaces
      val ignoreEnv = NETWORK_INTERFACE_CONTROLLER

      val rayArgs = RayArgs(
        mca             = ignoreEnv,
        // Aggregated number of virtual cores of compute nodes:
        // compute-0-* = (9 nodes, 8 virtual cores), compute-1-* = (5 nodes, 16 virtual cores)
        // (9 * 8) + (5 * 16) = 152
        procs           = "152",
        hostFilePath    = hostFilePath,
        kmerLength      = "31",
        leftSequence    = left,
        rightSequence   = right,
        minContigLength = "300",
        outputDirectory = nfsOutputPath
      )
      val rayCmd = buildCommand(rayArgs)
      println(s"\n\n${dateTime()} INFO RayWrapper: ${rayCmd.mkString(" ")}\n\n")

      val stdStreams = StdStreams("stdout", "stderr")

      val cmdIsSeq = false
      val redirect = true
      val exitCode = execCommand(rayCmd, envPath, cmdIsSeq, redirect, stdStreams)

      // Search for "panic" keyword in stdout as mpiexec does not return appropriate
      // exit codes upon failure
      val stdOutFile = scala.io.Source.fromFile(envPath + stdStreams.stdOut)
      try stdOutFile.getLines().foreach(line => {
        if(line.toLowerCase.contains("panic")) {
          exitFailure(rayCmd, exitCode, "PANIC", envPath)
        } else if(line.toLowerCase.contains("already exists")) {
          exitFailure(rayCmd, exitCode, "ALREADY EXISTS", envPath)
        }
      }) finally stdOutFile.close()

      moveDirectory(new File(nfsInputPath), new File(inputPath))
      moveDirectory(new File(nfsOutputPath), new File(outputPath))
      new File(nfsTmp).delete()

      exitSuccess(exitCode, toolContext.index, toolContext.uId)

      exitCode
    }

    override def output: RayOutput = {
      if(!new File(outputFilePath).exists) {
        return RayOutput(Seq.empty[FastaRecord])
      }

      // Read, parse, and return the produced contigs
      val contigs = new FastaReader(bufferedReaderFromFile(outputFilePath)).toSeq

      RayOutput(contigs = contigs)
    }

    override def command = Seq("mpirun")

    def buildCommand(args: RayArgs): Seq[String] = {
      val c = command :+
        "--np"                    :+ args.procs :+
        "--hostfile"              :+ args.hostFilePath :+
        "--mca"                   :+ args.mca :+
        toolContext.program       :+
        "-k"                      :+ args.kmerLength :+
        "-p"                      :+ args.leftSequence :+ args.rightSequence :+
        "-minimum-contig-length"  :+ args.minContigLength :+
        "-o"                      :+ args.outputDirectory
      c
    }

    def convertInputPath(inputPath: String, copyPath: String) = copyPath + inputPath.substring(inputPath.lastIndexOf("/") + 1)

  }
}
