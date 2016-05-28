package no.uit.sfb.toolwrapper

import java.io.File
import no.uit.sfb.toolabstraction.{StdStreams, ToolContext, ToolAbstraction, ToolFactory}


case class CatArgs(inputFilePath: String)
case class CatInput(inStrings: Seq[String])
case class CatOutput(outStrings: Seq[String])

class Cat extends ToolFactory[CatInput, CatOutput] {

  override def apply(toolContext: ToolContext, in: CatInput):
                    ToolAbstraction[CatOutput] = new ToolAbstraction[CatOutput] {

    envPath = toolContext.path

    inputPath = s"${envPath}catInput/"
    outputPath = s"${envPath}catOutput/"

    inputFilePath = s"${inputPath}cat.in_${toolContext.index}"
    outputFilePath = s"${outputPath}cat.out_${toolContext.index}"

    override def execute(): Int = {
      createToolDir(inputPath)
      createToolDir(outputPath)

      if(in.inStrings.isEmpty) {
        return EXIT_SUCCESS
      }

      // Write input to file to accommodate the tool
      val catInputFile = new File(inputFilePath)

      stringToFile(in.inStrings.mkString("\n"), inputFilePath)

      if(catInputFile.length() == 0) {
        return EXIT_SUCCESS
      }

      val catArgs = CatArgs(inputFilePath = inputFilePath)
      val catCmd = buildCommand(catArgs)
      val stdStreams = StdStreams(s"cat.out_${toolContext.index}", s"stderr_${toolContext.index}")
      val cmdIsSeq = true
      val redirect = true

      val exitCode = execCommand(catCmd, outputPath, cmdIsSeq, redirect, stdStreams)

      exitSuccess(exitCode, toolContext.index, toolContext.uId)

      exitCode
    }

    override def output: CatOutput = {
      var out = Seq.empty[String]

      if(!new File(outputFilePath).exists) {
        return CatOutput(out)
      }

      out = fileToLineSeq(outputFilePath)

      CatOutput(out)
    }

    override def command: Seq[String] = Seq(toolContext.program)

    def buildCommand(args: CatArgs): Seq[String] = {
      command :+ args.inputFilePath
    }

  }

}
