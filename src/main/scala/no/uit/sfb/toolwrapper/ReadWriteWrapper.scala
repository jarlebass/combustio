package no.uit.sfb.toolwrapper

import java.io.File
import no.uit.sfb.toolabstraction.{ToolContext, ToolAbstraction, ToolFactory}


case class ReadWriteInput(inStrings: Seq[String])
case class ReadWriteOutput(outStrings: Seq[String])

class ReadWrite extends ToolFactory[ReadWriteInput, ReadWriteOutput] {

  override def apply(toolContext: ToolContext, in: ReadWriteInput):
                    ToolAbstraction[ReadWriteOutput] = new ToolAbstraction[ReadWriteOutput] {

    envPath = toolContext.path

    inputPath = s"${envPath}readWriteInput/"
    outputPath = s"${envPath}readWriteOutput/"

    inputFilePath = s"${inputPath}readWrite.in_${toolContext.index}"
    outputFilePath = s"${outputPath}readWrite.out_${toolContext.index}"

    override def execute(): Int = {
      createToolDir(inputPath)
      createToolDir(outputPath)

      if(in.inStrings.isEmpty) {
        return EXIT_SUCCESS
      }

      // Write input to file to accommodate the tool
      val readWriteInputFile = new File(inputFilePath)

      stringToFile(in.inStrings.mkString("\n"), inputFilePath)

      if(readWriteInputFile.length() == 0) {
        return EXIT_SUCCESS
      }

      val out = fileToString(inputFilePath)
      stringToFile(out, outputFilePath)

      val exitCode = 0
      exitSuccess(exitCode, toolContext.index, toolContext.uId)

      exitCode
    }

    override def output: ReadWriteOutput = {
      var out = Seq.empty[String]

      if(!new File(outputFilePath).exists) {
        return ReadWriteOutput(out)
      }

      out = fileToLineSeq(outputFilePath)

      ReadWriteOutput(out)
    }

    override def command: Seq[String] = Seq(toolContext.program)

    override def validateBefore(help: String): Unit = Unit

  }

}
