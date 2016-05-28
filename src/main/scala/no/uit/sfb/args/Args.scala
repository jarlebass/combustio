package no.uit.sfb.args

import java.io._
import java.util.zip.{GZIPOutputStream, GZIPInputStream}
import com.beust.jcommander.Parameter

/**
 * Author: Inge Alexander Raknes
 */

trait SqliteDatabaseFileArg {
  @Parameter(
    names = Array("-d" ,"--database"),
    description = "SQLite3 database file")
  var sqliteDatabaseFile: String = "annotations.sqlite3"
}

trait BatchSizeArg {
  @Parameter(
    names = Array("--batch-size"),
    description = "Number of items in write buffer")
  var batchSize = 10000
}

object Args {
  def parseInputFile(name: String): InputStream = {
    if(name == "-") System.in
    else {
      val fileStream = new FileInputStream(name)
      if(name.endsWith(".gz")) new GZIPInputStream(fileStream)
      else fileStream
    }
  }

  def lineReader(filename: String): BufferedReader = {
    new BufferedReader(new InputStreamReader(parseInputFile(filename)))
  }

  def lineWriter(filename: String): PrintStream = {
    new PrintStream(new BufferedOutputStream(parseOutputFile(filename)))
  }

  def parseOutputFile(name: String): OutputStream = {
    if(name == "-") System.out
    else {
      val fileStream = new FileOutputStream(name)
      if(name.endsWith(".gz")) new GZIPOutputStream(fileStream)
      else fileStream
    }
  }

  def parseTags(tags: String): List[String] = {
    tags.split(',').toList.filter(!_.isEmpty)
  }
}
