package no.uit.sfb.toolwrapper

import java.io.File
import no.uit.sfb.toolabstraction.{StdStreams, ToolContext, ToolAbstraction, ToolFactory}


case class GrepArgs(inputFilePath: String, query: String)
case class GrepInput(inStrings: Seq[String], query: String)
case class GrepOutput(outStrings: Seq[String])

class Grep extends ToolFactory[GrepInput, GrepOutput] {

  override def apply(toolContext: ToolContext, in: GrepInput):
                    ToolAbstraction[GrepOutput] = new ToolAbstraction[GrepOutput] {

    envPath = toolContext.path

    inputPath = s"${envPath}grepInput/"
    outputPath = s"${envPath}grepOutput/"

    inputFilePath = s"${inputPath}grep.in_${toolContext.index}"
    outputFilePath = s"${outputPath}grep.out_${toolContext.index}"

    override def execute(): Int = {
      createToolDir(inputPath)
      createToolDir(outputPath)

      if(in.inStrings.isEmpty) {
        return EXIT_SUCCESS
      }

      // Write input to file to accommodate the tool
      val grepInputFile = new File(inputFilePath)

      stringToFile(in.inStrings.mkString("\n"), inputFilePath)

      if(grepInputFile.length() == 0) {
        return EXIT_SUCCESS
      }

      val grepArgs = GrepArgs(inputFilePath = inputFilePath, query = in.query)
      val grepCmd = buildCommand(grepArgs)
      val stdStreams = StdStreams(s"grep.out_${toolContext.index}", s"stderr_${toolContext.index}")
      val cmdIsSeq = true
      val redirect = true

      // grep returns exit status 1 if no matches are found, which is not an error to us
      val exitCode = _execCommand(grepCmd, outputPath, cmdIsSeq, redirect, stdStreams)
      if(exitCode == 2) {
        exitFailure(grepCmd, exitCode, "", envPath)
      }

      exitSuccess(exitCode, toolContext.index, toolContext.uId)

      exitCode
    }

    override def output: GrepOutput = {
      var out = Seq.empty[String]

      if(!new File(outputFilePath).exists) {
        return GrepOutput(out)
      }

      out = fileToLineSeq(outputFilePath)

      GrepOutput(out)
    }

    override def command: Seq[String] = Seq(toolContext.program)

    def buildCommand(args: GrepArgs): Seq[String] = {
      command :+
        args.query :+
        args.inputFilePath
    }

  }

}
