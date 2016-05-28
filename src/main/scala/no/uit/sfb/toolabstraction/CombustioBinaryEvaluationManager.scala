package no.uit.sfb.toolabstraction

import no.uit.sfb.toolwrapper._
import sys.process._
import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import com.beust.jcommander.Parameter
import no.uit.sfb.utils.FSUtils.createDirectory
import org.apache.spark.SparkContext
import no.uit.sfb.toolabstraction.CommandHelper._
import org.apache.spark.rdd.RDD

class CombustioBinaryEvaluationManager(context: => SparkContext) extends Command  {

  implicit lazy val sc = context

  override val args = new {
    @Parameter(names = Array("-lo", "--local-output-path"), required = true)
    var localOutputPath: String = _

    @Parameter(names = Array("-ho", "--hdfs-output-path"), required = true)
    var hdfsOutputPath: String = _

    @Parameter(names = Array("-j", "--job-id"), required = true)
    var jobId: String = _

    @Parameter(names = Array("-i", "--input-path"), required = true)
    var inputPath: String = _

    @Parameter(names = Array("-npart", "--num-partitions-multiplier"), required = true)
    var numPartitions: Int = _

    // Change if Spark configuration is altered, or files may be erroneously recovered
    @Parameter(names = Array("-u", "--unique-identification-number"), required = true)
    var uniqueId: String = _

    @Parameter(names = Array("-r", "--recovery"), required = true)
    var recovery: String = _
  }

  final val HDFS_SUCCESS_FILE_NAME = "_SUCCESS"
  final val QUERY = "dolor"

  var stageNumber = 0
  val hadoopConf = sc.hadoopConfiguration
  val hadoopFs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

  override def apply(): Unit = {
    println(s"\n\n${dateTime()} INFO CombustioBinaryEvaluationManager: sc.applicationId: ${sc.applicationId}\n" +
      s"${dateTime()} INFO CombustioBinaryEvaluationManager: inputPath: ${args.inputPath}\n")

    val RECOVERY: Boolean = if(args.recovery.toLowerCase == "y") true else false

    val uniqueId = args.uniqueId
    val inputPath = args.inputPath
    val localOutputPath = args.localOutputPath
    val hdfsOutputPath = args.hdfsOutputPath
    val jobPath = buildJobPath(localOutputPath, args.jobId)
    val hdfsJobPath = buildJobPath(hdfsOutputPath, args.jobId)
    val numPartitions = args.numPartitions

    // Solve permission-issues allowing executors to write in job directory on driver node
    createDirectory(jobPath)
    new File(jobPath).setWritable(true, false)

    // Validate tools on driver
    validatePrior(CAT, "--help")
    validatePrior(GREP, "--help")
    validatePrior(WC, "--help")

    val inputRDD: RDD[String] = sc.textFile(inputPath, sc.defaultParallelism * numPartitions)

    if(RECOVERY) {
      // cat
      val catToolPath = buildToolPath(CAT_DIR)
      val catPath = jobPath + catToolPath
      val catOutputRDD: RDD[CatOutput] = {
        val catOut = hdfsJobPath + catToolPath
        val catOutSuccess = catOut + HDFS_SUCCESS_FILE_NAME
        if (!hdfsFileExists(catOutSuccess)) {
          println(s"\n\n${dateTime()} INFO CombustioBinaryEvaluationManager: Could not recover cat output from HDFS, proceeding ...\n\n")
          val catOutput: RDD[CatOutput] = inputRDD.mapPartitionsWithIndex { (partitionIndex, inStrings) =>
            val catContext = new ToolContext {
              def index: Int = partitionIndex
              def help: String = "--help"
              def program: String = CAT
              def path: String = catPath
              def uId: String = uniqueId
            }

            val catWrapper = new ToolWrapperImpl(new Cat)
            val cat: CatInput => CatOutput = catWrapper(catContext)

            Iterator(cat(CatInput(inStrings.toSeq)))
          }

          catOutput.persist()

          if (hdfsFileExists(catOut)) {
            hadoopFs.delete(new org.apache.hadoop.fs.Path(catOut), true)
          }
          catOutput.saveAsObjectFile(catOut)
          catOutput
        } else {
          println(s"\n\n${dateTime()} INFO CombustioBinaryEvaluationManager: Recovering cat output from HDFS ...\n\n")
          val catOutput: RDD[CatOutput] = sc.objectFile(catOut, sc.defaultParallelism * numPartitions)
          catOutput
        }
      }


      // grep
      val grepToolPath = buildToolPath(GREP_DIR)
      val grepPath = jobPath + grepToolPath
      val grepOutputRDD: RDD[GrepOutput] = {
        val grepOut = hdfsJobPath + grepToolPath
        val grepOutSuccess = grepOut + HDFS_SUCCESS_FILE_NAME
        if (!hdfsFileExists(grepOutSuccess)) {
          println(s"\n\n${dateTime()} INFO CombustioBinaryEvaluationManager: Could not recover grep output from HDFS, proceeding ...\n\n")
          val grepOutput: RDD[GrepOutput] = catOutputRDD.flatMap(_.outStrings).mapPartitionsWithIndex { (partitionIndex, inStrings) =>
            val grepContext = new ToolContext {
              def index: Int = partitionIndex
              def help: String = "--help"
              def program: String = GREP
              def path: String = grepPath
              def uId: String = uniqueId
            }

            val grepWrapper = new ToolWrapperImpl(new Grep)
            val grep: GrepInput => GrepOutput = grepWrapper(grepContext)

            Iterator(grep(GrepInput(inStrings.toSeq, QUERY)))
          }

          grepOutput.persist()

          if (hdfsFileExists(grepOut)) {
            hadoopFs.delete(new org.apache.hadoop.fs.Path(grepOut), true)
          }
          grepOutput.saveAsObjectFile(grepOut)
          grepOutput
        } else {
          println(s"\n\n${dateTime()} INFO CombustioBinaryEvaluationManager: Recovering grep output from HDFS ...\n\n")
          val grepOutput: RDD[GrepOutput] = sc.objectFile(grepOut, sc.defaultParallelism * numPartitions)
          grepOutput
        }
      }


      // wc
      val wcToolPath = buildToolPath(WC_DIR)
      val wcPath = jobPath + wcToolPath
      val wcOutputRDD: RDD[WcOutput] = {
        val wcOut = hdfsJobPath + wcToolPath
        val wcOutSuccess = wcOut + HDFS_SUCCESS_FILE_NAME
        if (!hdfsFileExists(wcOutSuccess)) {
          println(s"\n\n${dateTime()} INFO CombustioBinaryEvaluationManager: Could not recover wc output from HDFS, proceeding ...\n\n")
          val wcOutput: RDD[WcOutput] = grepOutputRDD.flatMap(_.outStrings).mapPartitionsWithIndex { (partitionIndex, inStrings) =>
            val wcContext = new ToolContext {
              def index: Int = partitionIndex
              def help: String = "--help"
              def program: String = WC
              def path: String = wcPath
              def uId: String = uniqueId
            }

            val wcWrapper = new ToolWrapperImpl(new Wc)
            val wc: WcInput => WcOutput = wcWrapper(wcContext)

            Iterator(wc(WcInput(inStrings.toSeq)))
          }

          wcOutput.persist()

          if (hdfsFileExists(wcOut)) {
            hadoopFs.delete(new org.apache.hadoop.fs.Path(wcOut), true)
          }
          wcOutput.saveAsObjectFile(wcOut)
          wcOutput
        } else {
          println(s"\n\n${dateTime()} INFO CombustioBinaryEvaluationManager: Recovering wc output from HDFS ...\n\n")
          val wcOutput: RDD[WcOutput] = sc.objectFile(wcOut, sc.defaultParallelism * numPartitions)
          wcOutput
        }
      }

      val matches: Int = wcOutputRDD.filter(out => out.numWords.nonEmpty)
        .map(out => out.numWords.sum).reduce(_ + _)

      println(
        s"\n\n${dateTime()} INFO CombustioBinaryEvaluationManager: Words in lines containing $QUERY: " +
          s"$matches\n\n"
      )
    } else {
      // cat
      val catToolPath = buildToolPath(CAT_DIR)
      val catPath = jobPath + catToolPath
      val catOutputRDD: RDD[CatOutput] = inputRDD.mapPartitionsWithIndex { (partitionIndex, inStrings) =>
        val catContext = new ToolContext {
          def index: Int = partitionIndex
          def help: String = "--help"
          def program: String = CAT
          def path: String = catPath
          def uId: String = uniqueId
        }

        val catWrapper = new ToolWrapperImpl(new Cat)
        val cat: CatInput => CatOutput = catWrapper(catContext)

        Iterator(cat(CatInput(inStrings.toSeq)))
      }



      // grep
      val grepToolPath = buildToolPath(GREP_DIR)
      val grepPath = jobPath + grepToolPath
      val grepOutputRDD: RDD[GrepOutput] = catOutputRDD.flatMap(_.outStrings).mapPartitionsWithIndex { (partitionIndex, inStrings) =>
        val grepContext = new ToolContext {
          def index: Int = partitionIndex
          def help: String = "--help"
          def program: String = GREP
          def path: String = grepPath
          def uId: String = uniqueId
        }

        val grepWrapper = new ToolWrapperImpl(new Grep)
        val grep: GrepInput => GrepOutput = grepWrapper(grepContext)

        Iterator(grep(GrepInput(inStrings.toSeq, QUERY)))
      }



      // wc
      val wcToolPath = buildToolPath(WC_DIR)
      val wcPath = jobPath + wcToolPath
      val wcOutputRDD: RDD[WcOutput] = grepOutputRDD.flatMap(_.outStrings).mapPartitionsWithIndex { (partitionIndex, inStrings) =>
        val wcContext = new ToolContext {
          def index: Int = partitionIndex
          def help: String = "--help"
          def program: String = WC
          def path: String = wcPath
          def uId: String = uniqueId
        }

        val wcWrapper = new ToolWrapperImpl(new Wc)
        val wc: WcInput => WcOutput = wcWrapper(wcContext)

        Iterator(wc(WcInput(inStrings.toSeq)))
      }

      wcOutputRDD.persist()


      val matches: Int = wcOutputRDD.filter(out => out.numWords.nonEmpty)
        .map(out => out.numWords.sum).reduce(_ + _)

      println(
        s"\n\n${dateTime()} INFO CombustioBinaryEvaluationManager: Words in lines containing $QUERY: " +
          s"$matches\n\n"
      )
    }

  }

  def buildToolPath(tool: String): String = {
    val stagePath = s"stage_${this.stageNumber.toString}/"
    this.stageNumber += 1
    val toolPath = s"$stagePath$tool/"
    toolPath
  }

  def buildJobPath(outputPath: String, uniqueId: String): String = s"$outputPath$uniqueId/"


  def dateTime(): String = {
    new SimpleDateFormat("yy/MM/dd HH:mm:ss").format(Calendar.getInstance.getTime)
  }

  def hdfsFileExists(path: String): Boolean = {
    hadoopFs.exists(new org.apache.hadoop.fs.Path(path))
  }

  def convertInputPath(inputPath: String, copyPath: String) = {
    copyPath + inputPath.substring(inputPath.lastIndexOf("/") + 1)
  }

  def validatePrior(program: String, help: String) = {
    val exitCode: Int = Process(program + " " + help, new File("/tmp/")).run(new ProcessIO(in => in.close(), out => out.close(), err => err.close())).exitValue()

    if(exitCode == 127) {
      throw new Exception("The provided program is not available " +
        s"on the system.\nCmd: ${Seq(program, help).mkString(" ")}")
    }
  }
}
