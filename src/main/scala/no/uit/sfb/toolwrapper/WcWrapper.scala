package no.uit.sfb.toolwrapper

import java.io.File
import no.uit.sfb.toolabstraction.{StdStreams, ToolContext, ToolAbstraction, ToolFactory}


case class WcArgs(inputFilePath: String)
case class WcInput(inStrings: Seq[String])
case class WcOutput(numWords: Seq[Int])

class Wc extends ToolFactory[WcInput, WcOutput] {

  override def apply(toolContext: ToolContext, in: WcInput):
                    ToolAbstraction[WcOutput] = new ToolAbstraction[WcOutput] {

    envPath = toolContext.path

    inputPath = s"${envPath}wcInput/"
    outputPath = s"${envPath}wcOutput/"

    inputFilePath = s"${inputPath}wc.in_${toolContext.index}"
    outputFilePath = s"${outputPath}wc.out_${toolContext.index}"

    override def execute(): Int = {
      createToolDir(inputPath)
      createToolDir(outputPath)

      if(in.inStrings.isEmpty) {
        return EXIT_SUCCESS
      }

      // Write input to file to accommodate the tool
      val wcInputFile = new File(inputFilePath)

      stringToFile(in.inStrings.mkString("\n"), inputFilePath)

      if(wcInputFile.length() == 0) {
        return EXIT_SUCCESS
      }

      val wcArgs = WcArgs(inputFilePath = inputFilePath)
      val wcCmd = buildCommand(wcArgs)
      val stdStreams = StdStreams(s"wc.out_${toolContext.index}", s"stderr_${toolContext.index}")
      val cmdIsSeq = true
      val redirect = true

      val exitCode = execCommand(wcCmd, outputPath, cmdIsSeq, redirect, stdStreams)

      exitSuccess(exitCode, toolContext.index, toolContext.uId)

      exitCode
    }

    override def output: WcOutput = {
      var numWords = Seq.empty[Int]
      var rawOut = Seq.empty[String]

      if(!new File(outputFilePath).exists) {
        return WcOutput(numWords)
      }

      rawOut = fileToLineSeq(outputFilePath)

      rawOut.foreach(s => {
        val splits = s.split(" ")
        numWords :+= splits(0).toInt
      })

      WcOutput(numWords)
    }

    override def command: Seq[String] = Seq(toolContext.program)

    def buildCommand(args: WcArgs): Seq[String] = {
      command :+
        "-w"  :+
        args.inputFilePath
    }

  }

}
