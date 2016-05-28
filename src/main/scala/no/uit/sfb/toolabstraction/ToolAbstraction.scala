package no.uit.sfb.toolabstraction

import java.io._
import java.text.SimpleDateFormat
import java.util.Calendar
import com.google.common.io.Closer
import no.uit.sfb.utils.FSUtils
import scala.math.pow
import sys.process._
import FSUtils._

trait ToolContext {
  def program: String
  def path: String
  def index: Int
  def help: String
  def uId: String
}

trait ToolFactory[In, Out] {
  def apply(toolContext: ToolContext, in: In): ToolAbstraction[Out]
}

trait ToolWrapper[In, Out] {
  def apply(toolContext: ToolContext): (In => Out)
}

class ToolWrapperImpl[In, Out](toolFactory: ToolFactory[In, Out])
  extends ToolWrapper[In, Out] {

  override def apply(toolContext: ToolContext): (In) => Out = { in =>
    val tabs = toolFactory(toolContext, in)

    if(!tabs.recoverable(toolContext.index, toolContext.uId)) {
      tabs.validateBefore(toolContext.help)
      tabs.execute()
    }

    tabs.output
  }
}

case class StdStreams(stdOut: String, stdErr: String)

trait ToolAbstraction[Out] {

  final val EXIT_SUCCESS = 0
  final val EXIT_COMMAND_NOT_FOUND = 127
  final val SUCCESS_FILE_NAME = "STEP_SUCCESS"
  final val FAILURE_FILE_NAME = "STEP_FAILURE"

  // Buffer size for writing
  final val BUF_SIZE = pow(2, 12).toInt

  var envPath = "" : String
  var inputPath, inputFilePath = "" : String
  var outputPath, outputFilePath = "" : String

  def output: Out

  def command: Seq[String]

  def execute(): Int

  def recoverable(index: Int, uId: String): Boolean = {
    if(new File(successFilePath(index)).exists() &&
       new File(outputFilePath).exists())
    {

      try {
        val lines = fileToString(successFilePath(index)).split('\n')

        if(!(lines(0).toInt == EXIT_SUCCESS && lines(1) == uId)) {
          return false
        }
      } catch {
        case e: Exception => {
          println(e)
          return false
        }
      }

      println(s"\n\n${dateTime()} INFO ToolAbstraction: Recovering from local:\n" +
        s"\tsuccessFilePath: ${successFilePath(index)}\toutputFilePath: $outputFilePath\n\n")
      true
    } else {
      println(s"\n\n${dateTime()} INFO ToolAbstraction: Could not recover output from local" +
               " file system, must perform computation ...\n\n")
      false
    }
  }

  def validateBefore(help: String) = {
    val path = System.getProperty("java.io.tmpdir") + "/"
    val helpCmd = command :+ help
    val stdStreams = StdStreams("stdout", "stderr")
    val cmdIsSeq = true
    val redirect = false

    val exitCode = _execCommand(command :+ help, path, cmdIsSeq, redirect, stdStreams)
    if(exitCode == EXIT_COMMAND_NOT_FOUND) {
      throw new Exception("The provided tool command is not available " +
        s"on the system.\nCmd: $helpCmd\nPath: ")
    }
  }

  def createToolDir(toolPath: String): Boolean = {
    if(createDirectory(toolPath)) {
      new File(toolPath).setWritable(true, false)
      return true
    }
    false
  }

  def execCommand(cmd: Seq[String], envDir: String, cmdIsSeq: Boolean, redirect: Boolean,
                  stdStreams: StdStreams): (Int) = {
    val exitCode = _execCommand(cmd, envDir, cmdIsSeq, redirect, stdStreams)
    if(exitCode != EXIT_SUCCESS) {
      exitFailure(cmd, exitCode, "", envDir)
    }

    exitCode
  }

  def _execCommand(cmd: Seq[String], envPath: String, cmdIsSeq: Boolean, redirect: Boolean,
                   stdStreams: StdStreams): (Int) = {
    val envFile = new File(envPath)
    var proc: ProcessBuilder = null

    if(cmdIsSeq) proc = Process(cmd, envFile)
    else proc = Process(cmd.mkString(" "), envFile)

    val procIO = new ProcessIO(
      stdin => {
        stdin.close()
      },
      stdout => {
        if(redirect) {
          inputStreamToFile(stdout, envPath+stdStreams.stdOut)
        }

        stdout.close()
      },
      stderr => {
        if(redirect) {
          inputStreamToFile(stderr, envPath+stdStreams.stdErr)
        }

        stderr.close()
      }
    )

    proc.run(procIO).exitValue()
  }


  // From: http://stackoverflow.com/a/22663939
  def inputStreamToFile(is: InputStream, fileName: String) = {
    val closer = Closer.create()
    val buf: Array[Byte] = new Array[Byte](BUF_SIZE)
    var readLen: Int = 0

    try {
      val closerIn = closer.register(is)
      val closerOut = closer.register(new FileOutputStream(fileName))

      readLen = closerIn.read(buf)

      while (readLen != -1) {
        closerOut.write(buf, 0, readLen)
        readLen = closerIn.read(buf)
      }

      closerOut.flush()
    } finally {
      closer.close()
    }
  }

  def exitFailure(cmd: Seq[String], exitCode: Int, err: String,
                  stagePath: String) = {
    createFile(stagePath + FAILURE_FILE_NAME)
    scala.tools.nsc.io.File(stagePath + FAILURE_FILE_NAME).writeAll(exitCode.toString)
    throw new Exception(s"${dateTime()} Process exited unsuccessfully.\n" +
                        s"Cmd:\t\t${cmd.mkString(" ")}\n" +
                        s"Exit code:\t$exitCode\n" +
                        s"stderr:\t\t$err")
  }

  def successFilePath(id: Int): String = s"$envPath${SUCCESS_FILE_NAME}_$id"

  def exitSuccess(exitCode: Int, index: Int, uId: String) = {
    val path = successFilePath(index)
    createFile(path)
    scala.tools.nsc.io.File(path).writeAll(s"$exitCode\n$uId")
  }

  def dateTime(): String = {
    new SimpleDateFormat("yy/MM/dd hh:mm:ss").format(Calendar.getInstance.getTime)
  }

  def bufferedReaderFromString(s: String): BufferedReader = {
    new BufferedReader(new InputStreamReader(new ByteArrayInputStream(s.getBytes)))
  }

  def bufferedReaderFromFile(fileName: String): BufferedReader = {
    if(!new File(fileName).exists())
      throw new Exception("Cannot create BufferedReader from nonexisting file")
    new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))
  }

  def stringToFile(content: String, filename: String): Unit = {
    try {
      val writer = new BufferedWriter(new FileWriter(filename))
      writer.write(content)
      writer.close()
    }
  }

  def fileToString(fileName: String): String = {
    val file = scala.io.Source.fromFile(fileName)
    try file.getLines().mkString("\n") finally file.close()
  }

  def fileToLineSeq(fileName: String): Seq[String] = {
    var out = Seq.empty[String]
    val content = scala.io.Source.fromFile(fileName)
    try {
      val lines = content.getLines()
      while(lines.hasNext) {
        out :+= lines.next
      }
    } finally content.close()

    out
  }

}

