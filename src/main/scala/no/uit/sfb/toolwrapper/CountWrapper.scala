package no.uit.sfb.toolwrapper

import java.io.File
import no.uit.sfb.toolabstraction.{ToolContext, ToolAbstraction, ToolFactory}


case class CountInput(inStrings: Seq[String])
case class CountOutput(numWords: Seq[Int])

class Count extends ToolFactory[CountInput, CountOutput] {

  override def apply(toolContext: ToolContext, in: CountInput):
                    ToolAbstraction[CountOutput] = new ToolAbstraction[CountOutput] {

    envPath = toolContext.path

    inputPath = s"${envPath}countInput/"
    outputPath = s"${envPath}countOutput/"

    inputFilePath = s"${inputPath}count.in_${toolContext.index}"
    outputFilePath = s"${outputPath}count.out_${toolContext.index}"

    override def execute(): Int = {
      createToolDir(inputPath)
      createToolDir(outputPath)

      if(in.inStrings.isEmpty) {
        return EXIT_SUCCESS
      }

      val countInputFile = new File(inputFilePath)

      stringToFile(in.inStrings.mkString("\n"), inputFilePath)

      if(countInputFile.length() == 0) {
        return EXIT_SUCCESS
      }

      val out = fileToString(inputFilePath)
      stringToFile(out, outputFilePath)

      val exitCode = 0
      exitSuccess(exitCode, toolContext.index, toolContext.uId)

      exitCode
    }

    override def output: CountOutput = {
      var out = Seq.empty[Int]

      if(!new File(outputFilePath).exists) {
        return CountOutput(out)
      }

      out = fileToLineSeq(outputFilePath).map(_.split(" ").count(_.nonEmpty))

      CountOutput(out)
    }

    override def command: Seq[String] = Seq(toolContext.program)

    override def validateBefore(help: String): Unit = Unit

  }

}
