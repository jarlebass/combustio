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

class CombustioScalaEvaluationManager(context: => SparkContext) extends Command  {

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

    @Parameter(names = Array("-m", "--memory-only"), required = true)
    var memonly: String = _
  }

  final val HDFS_SUCCESS_FILE_NAME = "_SUCCESS"
  final val QUERY = "dolor"

  var stageNumber = 0
  val hadoopConf = sc.hadoopConfiguration
  val hadoopFs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

  override def apply(): Unit = {
    println(s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: sc.applicationId: ${sc.applicationId}\n" +
      s"${dateTime()} INFO CombustioScalaEvaluationManager: inputPath: ${args.inputPath}\n")

    val MEM_ONLY: Boolean = if(args.memonly.toLowerCase == "y") true else false
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

    val inputRDD: RDD[String] = sc.textFile(inputPath, sc.defaultParallelism * numPartitions)

    if(MEM_ONLY) {
      println(s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: MEM_ONLY\n")
      // readwritemem
      val readWriteToolPath = buildToolPath(READWRITE_DIR)
      val readWritePath = jobPath + readWriteToolPath
      val readWriteOutputRDD: RDD[ReadWriteOutput] = inputRDD.mapPartitionsWithIndex { (partitionIndex, inStrings) =>
        val readWriteContext = new ToolContext {
          def index: Int = partitionIndex
          def help: String = "n/a"
          def program: String = "n/a"
          def path: String = readWritePath
          def uId: String = uniqueId
        }

        val readWriteWrapper = new ToolWrapperImpl(new ReadWriteMemory)
        val readWrite: ReadWriteInput => ReadWriteOutput = readWriteWrapper(readWriteContext)

        Iterator(readWrite(ReadWriteInput(inStrings.toSeq)))
      }



      // filtermem
      val filterToolPath = buildToolPath(FILTER_DIR)
      val filterPath = jobPath + filterToolPath
      val filterOutputRDD: RDD[FilterOutput] = readWriteOutputRDD.flatMap(_.outStrings).mapPartitionsWithIndex { (partitionIndex, inStrings) =>
        val filterContext = new ToolContext {
          def index: Int = partitionIndex
          def help: String = "n/a"
          def program: String = "n/a"
          def path: String = filterPath
          def uId: String = uniqueId
        }

        val filterWrapper = new ToolWrapperImpl(new FilterMemory)
        val filter: FilterInput => FilterOutput = filterWrapper(filterContext)

        Iterator(filter(FilterInput(inStrings.toSeq, QUERY)))
      }


      // countmem
      val countToolPath = buildToolPath(COUNT_DIR)
      val countPath = jobPath + countToolPath
      val countOutputRDD: RDD[CountOutput] = filterOutputRDD.flatMap(_.outStrings).mapPartitionsWithIndex { (partitionIndex, inStrings) =>
        val countContext = new ToolContext {
          def index: Int = partitionIndex
          def help: String = "n/a"
          def program: String = "n/a"
          def path: String = countPath
          def uId: String = uniqueId
        }

        val countWrapper = new ToolWrapperImpl(new CountMemory)
        val count: CountInput => CountOutput = countWrapper(countContext)

        Iterator(count(CountInput(inStrings.toSeq)))
      }

      countOutputRDD.persist()


      val matches: Int = countOutputRDD.filter(out => out.numWords.nonEmpty)
        .map(out => out.numWords.sum).reduce(_ + _)

      println(
        s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: Words in lines containing $QUERY: " +
          s"$matches\n\n"
      )
    } else {
      println(s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: NOT MEM_ONLY\n")
      if (RECOVERY) {
        println(s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: RECOVERY\n")
        // readwrite
        val readWriteToolPath = buildToolPath(READWRITE_DIR)
        val readWritePath = jobPath + readWriteToolPath
        val readWriteOutputRDD: RDD[ReadWriteOutput] = {
          val readWriteOut = hdfsJobPath + readWriteToolPath
          val readWriteOutSuccess = readWriteOut + HDFS_SUCCESS_FILE_NAME
          if (!hdfsFileExists(readWriteOutSuccess)) {
            println(s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: Could not recover readwrite output from HDFS, proceeding ...\n\n")
            val readWriteOutput: RDD[ReadWriteOutput] = inputRDD.mapPartitionsWithIndex { (partitionIndex, inStrings) =>
              val readWriteContext = new ToolContext {
                def index: Int = partitionIndex
                def help: String = "n/a"
                def program: String = "n/a"
                def path: String = readWritePath
                def uId: String = uniqueId
              }

              val readWriteWrapper = new ToolWrapperImpl(new ReadWrite)
              val readWrite: ReadWriteInput => ReadWriteOutput = readWriteWrapper(readWriteContext)

              Iterator(readWrite(ReadWriteInput(inStrings.toSeq)))
            }


            if (hdfsFileExists(readWriteOut)) {
              hadoopFs.delete(new org.apache.hadoop.fs.Path(readWriteOut), true)
            }
            readWriteOutput.saveAsObjectFile(readWriteOut)
            readWriteOutput
          } else {
            println(s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: Recovering readwrite output from HDFS ...\n\n")
            val readWriteOutput: RDD[ReadWriteOutput] = sc.objectFile(readWriteOut, sc.defaultParallelism * numPartitions)
            readWriteOutput
          }
        }


        // filter
        val filterToolPath = buildToolPath(FILTER_DIR)
        val filterPath = jobPath + filterToolPath
        val filterOutputRDD: RDD[FilterOutput] = {
          val filterOut = hdfsJobPath + filterToolPath
          val filterOutSuccess = filterOut + HDFS_SUCCESS_FILE_NAME
          if (!hdfsFileExists(filterOutSuccess)) {
            println(s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: Could not recover filter output from HDFS, proceeding ...\n\n")
            val filterOutput: RDD[FilterOutput] = readWriteOutputRDD.flatMap(_.outStrings).mapPartitionsWithIndex { (partitionIndex, inStrings) =>
              val filterContext = new ToolContext {
                def index: Int = partitionIndex
                def help: String = "n/a"
                def program: String = "n/a"
                def path: String = filterPath
                def uId: String = uniqueId
              }

              val filterWrapper = new ToolWrapperImpl(new Filter)
              val filter: FilterInput => FilterOutput = filterWrapper(filterContext)

              Iterator(filter(FilterInput(inStrings.toSeq, QUERY)))
            }


            if (hdfsFileExists(filterOut)) {
              hadoopFs.delete(new org.apache.hadoop.fs.Path(filterOut), true)
            }
            filterOutput.saveAsObjectFile(filterOut)
            filterOutput
          } else {
            println(s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: Recovering filter output from HDFS ...\n\n")
            val filterOutput: RDD[FilterOutput] = sc.objectFile(filterOut, sc.defaultParallelism * numPartitions)
            filterOutput
          }
        }


        // count
        val countToolPath = buildToolPath(COUNT_DIR)
        val countPath = jobPath + countToolPath
        val countOutputRDD: RDD[CountOutput] = {
          val countOut = hdfsJobPath + countToolPath
          val countOutSuccess = countOut + HDFS_SUCCESS_FILE_NAME
          if (!hdfsFileExists(countOutSuccess)) {
            println(s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: Could not recover count output from HDFS, proceeding ...\n\n")
            val countOutput: RDD[CountOutput] = filterOutputRDD.flatMap(_.outStrings).mapPartitionsWithIndex { (partitionIndex, inStrings) =>
              val countContext = new ToolContext {
                def index: Int = partitionIndex
                def help: String = "n/a"
                def program: String = "n/a"
                def path: String = countPath
                def uId: String = uniqueId
              }

              val countWrapper = new ToolWrapperImpl(new Count)
              val count: CountInput => CountOutput = countWrapper(countContext)

              Iterator(count(CountInput(inStrings.toSeq)))
            }

            countOutput.persist()

            if (hdfsFileExists(countOut)) {
              hadoopFs.delete(new org.apache.hadoop.fs.Path(countOut), true)
            }
            countOutput.saveAsObjectFile(countOut)
            countOutput
          } else {
            println(s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: Recovering count output from HDFS ...\n\n")
            val countOutput: RDD[CountOutput] = sc.objectFile(countOut, sc.defaultParallelism * numPartitions)
            countOutput
          }
        }

        val matches: Int = countOutputRDD.filter(out => out.numWords.nonEmpty)
          .map(out => out.numWords.sum).reduce(_ + _)

        println(
          s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: Words in lines containing $QUERY: " +
            s"$matches\n\n"
        )
      } else {
        println(s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: NOT RECOVERY\n")
        // readwrite
        val readWriteToolPath = buildToolPath(READWRITE_DIR)
        val readWritePath = jobPath + readWriteToolPath
        val readWriteOutputRDD: RDD[ReadWriteOutput] = inputRDD.mapPartitionsWithIndex { (partitionIndex, inStrings) =>
          val readWriteContext = new ToolContext {
            def index: Int = partitionIndex
            def help: String = "n/a"
            def program: String = "n/a"
            def path: String = readWritePath
            def uId: String = uniqueId
          }

          val readWriteWrapper = new ToolWrapperImpl(new ReadWrite)
          val readWrite: ReadWriteInput => ReadWriteOutput = readWriteWrapper(readWriteContext)

          Iterator(readWrite(ReadWriteInput(inStrings.toSeq)))
        }



        // filter
        val filterToolPath = buildToolPath(FILTER_DIR)
        val filterPath = jobPath + filterToolPath
        val filterOutputRDD: RDD[FilterOutput] = readWriteOutputRDD.flatMap(_.outStrings).mapPartitionsWithIndex { (partitionIndex, inStrings) =>
          val filterContext = new ToolContext {
            def index: Int = partitionIndex
            def help: String = "n/a"
            def program: String = "n/a"
            def path: String = filterPath
            def uId: String = uniqueId
          }

          val filterWrapper = new ToolWrapperImpl(new Filter)
          val filter: FilterInput => FilterOutput = filterWrapper(filterContext)

          Iterator(filter(FilterInput(inStrings.toSeq, QUERY)))
        }


        // count
        val countToolPath = buildToolPath(COUNT_DIR)
        val countPath = jobPath + countToolPath
        val countOutputRDD: RDD[CountOutput] = filterOutputRDD.flatMap(_.outStrings).mapPartitionsWithIndex { (partitionIndex, inStrings) =>
          val countContext = new ToolContext {
            def index: Int = partitionIndex
            def help: String = "n/a"
            def program: String = "n/a"
            def path: String = countPath
            def uId: String = uniqueId
          }

          val countWrapper = new ToolWrapperImpl(new Count)
          val count: CountInput => CountOutput = countWrapper(countContext)

          Iterator(count(CountInput(inStrings.toSeq)))
        }

        countOutputRDD.persist()


        val matches: Int = countOutputRDD.filter(out => out.numWords.nonEmpty)
          .map(out => out.numWords.sum).reduce(_ + _)

        println(
          s"\n\n${dateTime()} INFO CombustioScalaEvaluationManager: Words in lines containing $QUERY: " +
            s"$matches\n\n"
        )
      }
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
