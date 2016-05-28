package no.uit.sfb.toolabstraction

import java.text.SimpleDateFormat
import java.util.Calendar
import com.beust.jcommander.Parameter
import org.apache.spark.SparkContext

class SparkImplManager(context: => SparkContext) extends Command  {

  implicit lazy val sc = context

  override val args = new {
    @Parameter(names = Array("-i", "--input-path"), required = true)
    var inputPath: String = _
  }

  final val QUERY = "dolor"

  override def apply(): Unit = {
    val inputPath = args.inputPath
    println(
      s"\n\n${dateTime()} INFO SparkImplManager: Words in lines containing $QUERY: " +
      sc.textFile(inputPath, sc.defaultParallelism)
        .filter(_.contains(QUERY))
        .map(_.split(" ").count(_.nonEmpty))
        .reduce(_ + _) +
      "\n\n"
    )
  }

  def dateTime(): String = {
    new SimpleDateFormat("yy/MM/dd HH:mm:ss").format(Calendar.getInstance.getTime)
  }
}
