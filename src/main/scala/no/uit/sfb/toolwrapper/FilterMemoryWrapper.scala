package no.uit.sfb.toolwrapper

import no.uit.sfb.toolabstraction.{ToolContext, ToolAbstraction, ToolFactory}

class FilterMemory extends ToolFactory[FilterInput, FilterOutput] {

  override def apply(toolContext: ToolContext, in: FilterInput):
                    ToolAbstraction[FilterOutput] = new ToolAbstraction[FilterOutput] {

    envPath = toolContext.path

    inputPath = s"${envPath}filterInput/"
    outputPath = s"${envPath}filterOutput/"

    inputFilePath = s"${inputPath}filter.in_${toolContext.index}"
    outputFilePath = s"${outputPath}filter.out_${toolContext.index}"

    var inStrings: Seq[String] = Seq.empty[String]

    override def execute(): Int = {
      if(in.inStrings.isEmpty) {
        return EXIT_SUCCESS
      }

      inStrings = in.inStrings
      inStrings = inStrings.filter(_.contains(in.query))

      0
    }

    override def output: FilterOutput = {
      FilterOutput(inStrings)
    }

    override def command: Seq[String] = Seq(toolContext.program)

    override def validateBefore(help: String): Unit = Unit

  }

}
