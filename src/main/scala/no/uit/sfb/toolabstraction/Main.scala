package no.uit.sfb.toolabstraction

import com.beust.jcommander.{Parameter, Parameters}
import org.apache.spark.{SparkConf, SparkContext}

trait Command extends (() => Unit) {
  def apply(): Unit

  @Parameters
  val args: Object
}

object Main extends App {

  @Parameters
  object arg {
    @Parameter(names = Array("--help", "-h"), help=true)
    var help = false
  }

  val sc = new SparkContext(new SparkConf().setAppName("Workflow"))

  val commands: Seq[(String, Command)] = Seq(
    "wm" -> new WorkflowManager(sc),
    "cbem" -> new CombustioBinaryEvaluationManager(sc),
    "csem" -> new CombustioScalaEvaluationManager(sc),
    "sim" -> new SparkImplManager(sc)
  )

  val jc = JCommanderFactory.get(arg)

  commands.foreach {
    case (name, cmd) => jc.addCommand(name, cmd.args)
  }

  jc.parse(args: _*)

  if(arg.help) {
    jc.usage()
    System.exit(1)
  } else {
    val commandName = jc.getParsedCommand
    val command = commands.collect { case (name, cmd) if name == commandName => cmd }
    command match {
      case Seq(cmd) =>
        cmd()
      case Seq() =>
        System.err.println("Undefined command.")
        System.exit(1)
      case _ =>
        System.err.println(s"More than 1 command with name '$command'")
        System.exit(1)
    }
  }
}
