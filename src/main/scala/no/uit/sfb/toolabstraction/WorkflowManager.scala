package no.uit.sfb.toolabstraction

import sys.process._
import java.io.File
import org.apache.hadoop.fs.Path
import java.text.SimpleDateFormat
import java.util.Calendar
import com.beust.jcommander.Parameter
import no.uit.sfb.args.Args
import no.uit.sfb.fasta.{FastaReader, FastaRecord}
import no.uit.sfb.toolwrapper._
import no.uit.sfb.utils.FSUtils.createDirectory
import org.apache.spark.SparkContext
import no.uit.sfb.toolabstraction.CommandHelper._
import org.apache.spark.rdd.RDD

class WorkflowManager(context: => SparkContext) extends Command  {

  implicit lazy val sc = context

  override val args = new {
    @Parameter(names = Array("-lo", "--local-output-path"), required = true)
    var localOutputPath: String = _

    @Parameter(names = Array("-ho", "--hdfs-output-path"), required = true)
    var hdfsOutputPath: String = _

    @Parameter(names = Array("-j", "--job-id"), required = true)
    var jobId: String = _

    @Parameter(names = Array("-l", "--left-fastq-path"), required = true)
    var leftFastqPath: String = _

    @Parameter(names = Array("-r", "--right-fastq-path"), required = true)
    var rightFastqPath: String = _

    // Change if Spark configuration is altered, or files may be erroneously recovered
    @Parameter(names = Array("-u", "--unique-identification-number"), required = true)
    var uniqueId: String = _

    @Parameter(names = Array("-R", "--recovery"), required = true)
    var recovery: String = _
  }

  final val HDFS_SUCCESS_FILE_NAME = "_SUCCESS"
  final val DEBUG: Boolean = false

  var stageNumber = 0
  val hadoopConf = sc.hadoopConfiguration
  val hadoopFs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

  override def apply(): Unit = {
    println(s"\n\n${dateTime()} INFO WorkflowManager: sc.applicationId: ${sc.applicationId}\n" +
      s"${dateTime()} INFO WorkflowManager: leftFastqPath: ${args.leftFastqPath}\n" +
      s"${dateTime()} INFO WorkflowManager: rightFastqPath: ${args.rightFastqPath}\n\n")

    val RECOVERY: Boolean = if(args.recovery.toLowerCase == "y") true else false

    val uniqueId = args.uniqueId
    val localOutputPath = args.localOutputPath
    val hdfsOutputPath = args.hdfsOutputPath
    val jobPath = buildJobPath(localOutputPath, args.jobId)
    //val jobPath = buildJobPath(localOutputPath, sc.applicationId)
    val hdfsJobPath = buildJobPath(hdfsOutputPath, args.jobId)

    val BLAST_DB_SIZE_FILE = jobPath + "blast_db_size"

    // Solve permission-issues allowing executors to write in job directory on driver node
    createDirectory(jobPath)
    new File(jobPath).setWritable(true, false)

    // Validate tools on driver
    validatePrior(RAY, "-help")
    validatePrior(MGA, "assertExitCodeNot127")
    validatePrior(BLAST, "-help")
    validatePrior(INTERPROSCAN5_COMPUTE0, "assertExitCodeNot127")

    if(RECOVERY) {
      println(s"\n\n${dateTime()} INFO WorkflowManager: RECOVERY ENABLED\n\n")
      // Ray
      val rayToolPath = buildToolPath(RAY_DIR)
      val rayPath = jobPath + rayToolPath
      val rayContigs: RDD[FastaRecord] = {
        val rayOut = hdfsJobPath + rayToolPath
        val rayOutSuccess = rayOut + HDFS_SUCCESS_FILE_NAME
        if (!hdfsFileExists(rayOutSuccess)) {
          println(s"\n\n${dateTime()} INFO WorkflowManager: Could not recover Ray output from HDFS, proceeding ...\n\n")
          val rayStartTime = System.nanoTime()

          hadoopFs.copyToLocalFile(new Path(args.leftFastqPath), new Path(localOutputPath))
          hadoopFs.copyToLocalFile(new Path(args.rightFastqPath), new Path(localOutputPath))
          val rayInput: RayInput = RayInput(
            leftInputFilePath = convertInputPath(args.leftFastqPath, localOutputPath),
            rightInputFilePath = convertInputPath(args.rightFastqPath, localOutputPath)
          )

          val rayContext = new ToolContext {
            def index: Int = 0
            def help: String = "-help"
            def program: String = RAY
            def path: String = rayPath
            def uId: String = uniqueId
          }

          val rayWrapper = new ToolWrapperImpl(new Ray)
          val ray: RayInput => RayOutput = rayWrapper(rayContext)

          // Run on driver, i.e., Spark does not track time
          val rayOutput: RayOutput = ray(rayInput)
          val rayContigs: RDD[FastaRecord] = sc.parallelize(rayOutput.contigs, sc.defaultParallelism * 3)
            .cache()

          val rayFinishTime = System.nanoTime()
          println(s"\n\n${dateTime()} INFO WorkflowManager: Ray from local took ${(rayFinishTime - rayStartTime) / 1e9} seconds to finish\n\n")

          if (hdfsFileExists(rayOut)) {
            hadoopFs.delete(new org.apache.hadoop.fs.Path(rayOut), true)
          }
          rayContigs.saveAsObjectFile(rayOut)
          rayContigs
        } else {
          println(s"\n\n${dateTime()} INFO WorkflowManager: Recovering Ray output from HDFS ...\n\n")
          val rayStartTime = System.nanoTime()

          val rayContigs: RDD[FastaRecord] = sc.objectFile(rayOut, sc.defaultParallelism * 3)

          val rayFinishTime = System.nanoTime()
          println(s"\n\n${dateTime()} INFO WorkflowManager: Loading Ray output from HDFS took ${(rayFinishTime - rayStartTime) / 1e9} seconds to finish\n\n")

          rayContigs.cache()
          rayContigs
        }
      }

      if (DEBUG) {
        println(s"\n\n${dateTime()} INFO WorkflowManager: rayContigs.count():\t${rayContigs.count()}\n\n")
        rayContigs.take(5).foreach(println)
      }


      // MGA
      val mgaToolPath = buildToolPath(MGA_DIR)
      val mgaPath = jobPath + mgaToolPath
      val mgaOutputRDD: RDD[MgaOutput] = {
        val mgaOut = hdfsJobPath + mgaToolPath
        val mgaOutSuccess = mgaOut + HDFS_SUCCESS_FILE_NAME
        if (!hdfsFileExists(mgaOutSuccess)) {
          println(s"\n\n${dateTime()} INFO WorkflowManager: Could not recover MGA output from HDFS, proceeding ...\n\n")
          val mgaOutput: RDD[MgaOutput] = rayContigs.mapPartitionsWithIndex { (partitionIndex, contigs) =>
            val mgaContext = new ToolContext {
              def index: Int = partitionIndex
              def help: String = "assertExitCodeNot127"
              def program: String = MGA
              def path: String = mgaPath
              def uId: String = uniqueId
            }

            val mgaWrapper = new ToolWrapperImpl(new Mga)
            val mga: MgaInput => MgaOutput = mgaWrapper(mgaContext)

            Iterator(mga(MgaInput(contigs = contigs.toSeq)))
          }

          mgaOutput.cache()

          if (hdfsFileExists(mgaOut)) {
            hadoopFs.delete(new org.apache.hadoop.fs.Path(mgaOut), true)
          }
          mgaOutput.saveAsObjectFile(mgaOut)
          mgaOutput
        } else {
          println(s"\n\n${dateTime()} INFO WorkflowManager: Recovering MGA output from HDFS ...\n\n")
          val mgaOutput: RDD[MgaOutput] = sc.objectFile(mgaOut, sc.defaultParallelism * 3)
          mgaOutput
        }
      }

      rayContigs.unpersist()

      if (DEBUG) {
        println(s"\n\n${dateTime()} INFO WorkflowManager: mgaOutputRDD.count():\t${mgaOutputRDD.count()}\n\n")
        mgaOutputRDD.first().nucleotideGenes.take(5).foreach(println)
      }

      val mgaNucleotides: RDD[FastaRecord] = mgaOutputRDD.flatMap { mgaOutput => mgaOutput.nucleotideGenes }

      if (DEBUG) {
        println(s"\n\n${dateTime()} INFO WorkflowManager: mgaNucleotides.count():\t${mgaNucleotides.count()}\n\n")
        println(s"\n\n${dateTime()} INFO WorkflowManager: mgaNucleotides.distinct().count():\t${mgaNucleotides.count()}\n\n")
        mgaNucleotides.take(5).foreach(println)
      }


      // BLAST
      val blastDbSourceSizeCountStartTime = System.nanoTime()
      val blastDbSourceSize: Long = {
        if (new File(BLAST_DB_SIZE_FILE).exists) {
          println(s"\n\n${dateTime()} INFO WorkflowManager: Recovering BLAST database size\n\n")
          val content = scala.io.Source.fromFile(BLAST_DB_SIZE_FILE)
          try {
            content.getLines().mkString("\n").toLong
          } finally content.close()
        } else {
          println(s"\n\n${dateTime()} INFO WorkflowManager: Could not recover BLAST database size, counting ...\n\n")
          val dbSize: Long = new FastaReader(Args.lineReader(BLAST_DB_SOURCE)).map(rec => rec.sequence.length()).sum
          scala.tools.nsc.io.File(BLAST_DB_SIZE_FILE).writeAll(dbSize.toString)
          dbSize
        }
      }
      val blastDbSourceSizeCountFinishTime = System.nanoTime()
      println(s"\n\n${dateTime()} INFO WorkflowManager: Length of all sequences in source FASTA: $blastDbSourceSize")
      println(s"${dateTime()} INFO WorkflowManager: Counting cumulative length of all sequences in the source FASTA of " +
        s"the BLAST database took " +
        s"${(blastDbSourceSizeCountFinishTime - blastDbSourceSizeCountStartTime) / 1e9} " +
        "seconds to finish\n\n")

      val blastToolPath = buildToolPath(BLAST_DIR)
      val blastPath = jobPath + blastToolPath
      val blastOutputRDD: RDD[BlastOutput] = {
        val blastOut = hdfsJobPath + blastToolPath
        val blastOutSuccess = blastOut + HDFS_SUCCESS_FILE_NAME
        if (!hdfsFileExists(blastOutSuccess)) {
          println(s"\n\n${dateTime()} INFO WorkflowManager: Could not recover BLAST output from HDFS, proceeding ...\n\n")
          val blastOutput: RDD[BlastOutput] = mgaNucleotides.mapPartitionsWithIndex { (partitionIndex, predictedSequences) =>
            val blastContext = new ToolContext {
              def index: Int = partitionIndex
              def help: String = "-help"
              def program: String = BLAST
              def path: String = blastPath
              def uId: String = uniqueId
            }

            val blastWrapper = new ToolWrapperImpl(new Blast)
            val blast: BlastInput => BlastOutput = blastWrapper(blastContext)

            Iterator(blast(BlastInput(predictedSequences.toSeq, blastDbSourceSize)))
          }

          blastOutput.cache()

          if (hdfsFileExists(blastOut)) {
            hadoopFs.delete(new org.apache.hadoop.fs.Path(blastOut), true)
          }
          blastOutput.saveAsObjectFile(blastOut)
          blastOutput
        } else {
          println(s"\n\n${dateTime()} INFO WorkflowManager: Recovering BLAST output from HDFS ...\n\n")
          val blastOutput: RDD[BlastOutput] = sc.objectFile(blastOut, sc.defaultParallelism * 3)
          blastOutput
        }
      }

      if (DEBUG) {
        val blastRecordCount: Int = blastOutputRDD.map(_.blastRecords.size).reduce(_ + _)
        println(s"\n\n${dateTime()} INFO WorkflowManager: blastOutputRDD.map(_.blastRecords.size).reduce(_ + _):\t${blastRecordCount}\n\n")
        blastOutputRDD.take(1).foreach(b => b.blastRecords.foreach(println))
      }

      // TODO: Sort and determine best hits?

      blastOutputRDD.unpersist()


      // InterProScan 5
      val interProScan5ToolPath = buildToolPath(INTERPROSCAN5_DIR)
      val interProScan5Path = jobPath + interProScan5ToolPath
      val interProScan5OutputRDD: RDD[InterProScan5Output] = {
        val interProScan5Out = hdfsJobPath + interProScan5ToolPath
        val interProScan5OutSuccess = interProScan5Out + HDFS_SUCCESS_FILE_NAME
        if (!hdfsFileExists(interProScan5OutSuccess)) {
          println(s"\n\n${dateTime()} INFO WorkflowManager: Could not recover InterProScan 5 output from HDFS, proceeding ...\n\n")
          val interProScan5Output: RDD[InterProScan5Output] = mgaNucleotides.mapPartitionsWithIndex { (partitionIndex, predictedSequences) =>
            val interProScan5Context = new ToolContext {
              def index: Int = partitionIndex
              def help: String = "assertExitCodeNot127"
              def program: String = INTERPROSCAN5_COMPUTE0
              def path: String = interProScan5Path
              def uId: String = uniqueId
            }

            val interProScan5Wrapper = new ToolWrapperImpl(new InterProScan5)
            val interProScan5: InterProScan5Input => InterProScan5Output = interProScan5Wrapper(interProScan5Context)

            Iterator(interProScan5(InterProScan5Input(predictedSequences.toSeq)))
          }

          interProScan5Output.cache()

          if (hdfsFileExists(interProScan5Out)) {
            hadoopFs.delete(new org.apache.hadoop.fs.Path(interProScan5Out), true)
          }
          interProScan5Output.saveAsObjectFile(interProScan5Out)
          interProScan5Output
        } else {
          println(s"\n\n${dateTime()} INFO WorkflowManager: Recovering InterProScan 5 output from HDFS ...\n\n")
          val interProScan5Output: RDD[InterProScan5Output] = sc.objectFile(interProScan5Out, sc.defaultParallelism * 3)
          interProScan5Output
        }
      }

      mgaOutputRDD.unpersist()

      val interProRecordCount: Int = interProScan5OutputRDD.map(_.interProRecords.size).reduce(_ + _)
      println(s"\n\n${dateTime()} INFO WorkflowManager: interProScan5OutputRDD.map(_.interProRecords.size).reduce(_ + _):\t${interProRecordCount}\n\n")
      interProScan5OutputRDD.take(5).foreach(r => r.interProRecords.foreach(println))

      interProScan5OutputRDD.unpersist()
    } else {
      println(s"\n\n${dateTime()} INFO WorkflowManager: RECOVERY DISABLED\n\n")

      // Ray
      val rayToolPath = buildToolPath(RAY_DIR)
      val rayPath = jobPath + rayToolPath
      val rayContigs: RDD[FastaRecord] = {

          hadoopFs.copyToLocalFile(new Path(args.leftFastqPath), new Path(localOutputPath))
          hadoopFs.copyToLocalFile(new Path(args.rightFastqPath), new Path(localOutputPath))
          val rayInput: RayInput = RayInput(
            leftInputFilePath = convertInputPath(args.leftFastqPath, localOutputPath),
            rightInputFilePath = convertInputPath(args.rightFastqPath, localOutputPath)
          )

          val rayContext = new ToolContext {
            def index: Int = 0
            def help: String = "-help"
            def program: String = RAY
            def path: String = rayPath
            def uId: String = uniqueId
          }

          val rayWrapper = new ToolWrapperImpl(new Ray)
          val ray: RayInput => RayOutput = rayWrapper(rayContext)

          // Run on driver, i.e., Spark does not track time
          val rayOutput: RayOutput = ray(rayInput)
          val rayContigs: RDD[FastaRecord] = sc.parallelize(rayOutput.contigs, sc.defaultParallelism * 3)
            .cache()


          rayContigs
      }

      // MGA
      val mgaToolPath = buildToolPath(MGA_DIR)
      val mgaPath = jobPath + mgaToolPath
      val mgaOutputRDD: RDD[MgaOutput] = rayContigs.mapPartitionsWithIndex { (partitionIndex, contigs) =>
            val mgaContext = new ToolContext {
              def index: Int = partitionIndex
              def help: String = "assertExitCodeNot127"
              def program: String = MGA
              def path: String = mgaPath
              def uId: String = uniqueId
            }

            val mgaWrapper = new ToolWrapperImpl(new Mga)
            val mga: MgaInput => MgaOutput = mgaWrapper(mgaContext)

            Iterator(mga(MgaInput(contigs = contigs.toSeq)))
      }

      mgaOutputRDD.cache()

      rayContigs.unpersist()

      if (DEBUG) {
        println(s"\n\n${dateTime()} INFO WorkflowManager: mgaOutputRDD.count():\t${mgaOutputRDD.count()}\n\n")
        mgaOutputRDD.first().nucleotideGenes.take(5).foreach(println)
      }

      val mgaNucleotides: RDD[FastaRecord] = mgaOutputRDD.flatMap { mgaOutput => mgaOutput.nucleotideGenes }

      if (DEBUG) {
        println(s"\n\n${dateTime()} INFO WorkflowManager: mgaNucleotides.count():\t${mgaNucleotides.count()}\n\n")
        println(s"\n\n${dateTime()} INFO WorkflowManager: mgaNucleotides.distinct().count():\t${mgaNucleotides.count()}\n\n")
        mgaNucleotides.take(5).foreach(println)
      }


      // BLAST
      val blastDbSourceSizeCountStartTime = System.nanoTime()
      val blastDbSourceSize: Long = {
          println(s"\n\n${dateTime()} INFO WorkflowManager: Could not recover BLAST database size, counting ...\n\n")
          val dbSize: Long = new FastaReader(Args.lineReader(BLAST_DB_SOURCE)).map(rec => rec.sequence.length()).sum
          scala.tools.nsc.io.File(BLAST_DB_SIZE_FILE).writeAll(dbSize.toString)
          dbSize
      }

      val blastDbSourceSizeCountFinishTime = System.nanoTime()
      println(s"\n\n${dateTime()} INFO WorkflowManager: Length of all sequences in source FASTA: $blastDbSourceSize")
      println(s"${dateTime()} INFO WorkflowManager: Counting cumulative length of all sequences in the source FASTA of " +
        s"the BLAST database took " +
        s"${(blastDbSourceSizeCountFinishTime - blastDbSourceSizeCountStartTime) / 1e9} " +
        "seconds to finish\n\n")

      val blastToolPath = buildToolPath(BLAST_DIR)
      val blastPath = jobPath + blastToolPath
      val blastOutputRDD: RDD[BlastOutput] = mgaNucleotides.mapPartitionsWithIndex { (partitionIndex, predictedSequences) =>
        val blastContext = new ToolContext {
          def index: Int = partitionIndex
          def help: String = "-help"
          def program: String = BLAST
          def path: String = blastPath
          def uId: String = uniqueId
        }

        val blastWrapper = new ToolWrapperImpl(new Blast)
        val blast: BlastInput => BlastOutput = blastWrapper(blastContext)

        Iterator(blast(BlastInput(predictedSequences.toSeq, blastDbSourceSize)))
      }

      blastOutputRDD.cache()


      if (DEBUG) {
        val blastRecordCount: Int = blastOutputRDD.map(_.blastRecords.size).reduce(_ + _)
        println(s"\n\n${dateTime()} INFO WorkflowManager: blastOutputRDD.map(_.blastRecords.size).reduce(_ + _):\t${blastRecordCount}\n\n")
        blastOutputRDD.take(1).foreach(b => b.blastRecords.foreach(println))
      }

      // TODO: Sort and determine best hits?

      blastOutputRDD.count()
      blastOutputRDD.unpersist()


      // InterProScan 5
      val interProScan5ToolPath = buildToolPath(INTERPROSCAN5_DIR)
      val interProScan5Path = jobPath + interProScan5ToolPath
      val interProScan5OutputRDD: RDD[InterProScan5Output] = mgaNucleotides.mapPartitionsWithIndex { (partitionIndex, predictedSequences) =>
        val interProScan5Context = new ToolContext {
          def index: Int = partitionIndex
          def help: String = "assertExitCodeNot127"
          def program: String = INTERPROSCAN5_COMPUTE0
          def path: String = interProScan5Path
          def uId: String = uniqueId
        }

        val interProScan5Wrapper = new ToolWrapperImpl(new InterProScan5)
        val interProScan5: InterProScan5Input => InterProScan5Output = interProScan5Wrapper(interProScan5Context)

        Iterator(interProScan5(InterProScan5Input(predictedSequences.toSeq)))
      }

      interProScan5OutputRDD.cache()

      mgaOutputRDD.unpersist()

      val interProRecordCount: Int = interProScan5OutputRDD.map(_.interProRecords.size).reduce(_ + _)
      println(s"\n\n${dateTime()} INFO WorkflowManager: interProScan5OutputRDD.map(_.interProRecords.size).reduce(_ + _):\t${interProRecordCount}\n\n")
      interProScan5OutputRDD.take(5).foreach(r => r.interProRecords.foreach(println))

      interProScan5OutputRDD.unpersist()
    }

    // STOP? sc.stop()
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
