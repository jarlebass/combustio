package no.uit.sfb.toolwrapper

import no.uit.sfb.toolabstraction.{ToolContext, ToolAbstraction, ToolFactory}


class CountMemory extends ToolFactory[CountInput, CountOutput] {

  override def apply(toolContext: ToolContext, in: CountInput):
                    ToolAbstraction[CountOutput] = new ToolAbstraction[CountOutput] {

    envPath = toolContext.path

    inputPath = s"${envPath}countInput/"
    outputPath = s"${envPath}countOutput/"

    inputFilePath = s"${inputPath}count.in_${toolContext.index}"
    outputFilePath = s"${outputPath}count.out_${toolContext.index}"

    var inStrings: Seq[String] = Seq.empty[String]

    override def execute(): Int = {
      if(in.inStrings.isEmpty) {
        return EXIT_SUCCESS
      }

      inStrings = in.inStrings

      0
    }

    override def output: CountOutput = {
      var out = Seq.empty[Int]

      if(inStrings.isEmpty) {
        return CountOutput(out)
      }

      out = inStrings.map(_.split(" ").count(_.nonEmpty))

      CountOutput(out)
    }

    override def command: Seq[String] = Seq(toolContext.program)

    override def validateBefore(help: String): Unit = Unit

  }

}
