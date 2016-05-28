package no.uit.sfb.toolwrapper

import java.io.File
import no.uit.sfb.toolabstraction.{ToolContext, ToolAbstraction, ToolFactory}


case class FilterInput(inStrings: Seq[String], query: String)
case class FilterOutput(outStrings: Seq[String])

class Filter extends ToolFactory[FilterInput, FilterOutput] {

  override def apply(toolContext: ToolContext, in: FilterInput):
                    ToolAbstraction[FilterOutput] = new ToolAbstraction[FilterOutput] {

    envPath = toolContext.path

    inputPath = s"${envPath}filterInput/"
    outputPath = s"${envPath}filterOutput/"

    inputFilePath = s"${inputPath}filter.in_${toolContext.index}"
    outputFilePath = s"${outputPath}filter.out_${toolContext.index}"

    override def execute(): Int = {
      createToolDir(inputPath)
      createToolDir(outputPath)

      if(in.inStrings.isEmpty) {
        return EXIT_SUCCESS
      }

      // Write input to file to accommodate the tool
      val filterInputFile = new File(inputFilePath)

      stringToFile(in.inStrings.mkString("\n"), inputFilePath)

      if(filterInputFile.length() == 0) {
        return EXIT_SUCCESS
      }

      val out = fileToLineSeq(inputFilePath).filter(_.contains(in.query))
      stringToFile(out.mkString("\n"), outputFilePath)

      val exitCode = 0
      exitSuccess(exitCode, toolContext.index, toolContext.uId)

      exitCode
    }

    override def output: FilterOutput = {
      var out = Seq.empty[String]

      if(!new File(outputFilePath).exists) {
        return FilterOutput(out)
      }

      out = fileToLineSeq(outputFilePath)

      FilterOutput(out)
    }

    override def command: Seq[String] = Seq(toolContext.program)

    override def validateBefore(help: String): Unit = Unit

  }

}
